"""Jobs on the gateway's launcher engine: batch jobs from the catalog, run as pods in your namespace.

    with KyuubiClient(url, user="alice", password=pw) as client, Launcher(client) as jobs:
        jobs.list()                                    # the catalog
        result = jobs.run("hello", seconds="5", memory="256Mi",
                          on_line=print)               # submit, follow the log, wait; returns the final status
        run = jobs.submit("hello", seconds="60")       # or: start it and come back later
        jobs.status(run); jobs.logs(run); jobs.cancel(run)

The server side is LauncherDialect in this repo's kyuubi-launcher module (statements RUN, STATUS, LOGS,
CANCEL, SHOW JOBS, SHOW RUNS; docs/launcher.md). `run` submits with async=true and follows the log by polling, so no HTTP
request stays open for the length of the job and a lost connection does not lose the run: `follow(run)`
picks it up again.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable

from . import errors
from .client import KyuubiClient, Session

JOB = re.compile(r"[a-z0-9-]+")
PARAM = re.compile(r"[a-z_]+")
VALUE = re.compile(r"[A-Za-z0-9_.,:/=+-]{1,200}")
DONE = {"Succeeded", "Failed"}


class RunFailed(errors.Error):
    """A job run that ended Failed (or was cancelled). `.status` is its final STATUS row."""

    def __init__(self, status: dict):
        self.status = status
        super().__init__(f"run {status.get('run')} {status.get('phase', 'failed')} with exit code "
                         f"{status.get('exit_code')}" + (f" ({status['message']})" if status.get("message") else ""))


class Launcher:
    def __init__(self, client: KyuubiClient):
        self.client = client
        self.session: Session | None = None

    # -- session
    def __enter__(self) -> "Launcher":
        self.session = self.client.open_session(None, {"kyuubi.engine.jdbc.type": "launcher"})
        return self

    def __exit__(self, *exc):
        if self.session:
            self.session.close()
            self.session = None

    def _query(self, statement: str, timeout: float = 600) -> list[dict]:
        if self.session is None:
            raise errors.InterfaceError("use Launcher as a context manager")
        op = self.session.execute(statement, wait=True, timeout=timeout)
        try:
            names = [c["columnName"] for c in op.columns]
            return [dict(zip(names, row)) for row in op.rows()]
        finally:
            op.close()

    # -- the catalog and the runs
    def list(self) -> list[dict]:
        return self._query("SHOW JOBS")

    def runs(self) -> list[dict]:
        return self._query("SHOW RUNS")

    def submit(self, job: str, **params) -> str:
        """Starts the job and returns its run id at once. Parameters are the job's declared ones, as strings."""
        if not JOB.fullmatch(job):
            raise errors.InterfaceError(f"bad job name {job!r}")
        words = []
        for name, value in params.items():
            if not PARAM.fullmatch(name) or not VALUE.fullmatch(str(value)):
                raise errors.InterfaceError(f"bad parameter {name}={value!r}: letters, digits and _.,:/=+- only")
            words.append(f"{name}={value}")
        return self._query(f"RUN {job} async=true {' '.join(words)}".strip())[0]["run"]

    def status(self, run: str) -> dict:
        return self._query(f"STATUS {run}")[0]

    def logs(self, run: str, from_line: int = 1) -> list[tuple[int, str]]:
        return [(r["n"], r["line"]) for r in self._query(f"LOGS {run} from={from_line}")]

    def cancel(self, run: str) -> None:
        self._query(f"CANCEL {run}")

    # -- waiting
    def follow(self, run: str, on_line: Callable[[str], None] | None = None, poll: float = 2.0,
               timeout: float | None = None) -> dict:
        """Tails the run's log (each new line to on_line) until it ends, and returns its final STATUS row.
        Raises RunFailed if it failed or was cancelled; Ctrl-C leaves the run going (cancel it explicitly)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        nxt = 1
        while True:
            try:
                status = self.status(run)
            except errors.QueryError as e:
                if "not found" in str(e).lower() or "404" in str(e):
                    raise RunFailed({"run": run, "phase": "cancelled", "exit_code": -1,
                                     "message": "its pod is gone"}) from None
                raise
            for n, line in self.logs(run, nxt):
                if on_line:
                    on_line(line)
                nxt = n + 1
            if status["phase"] in DONE:
                for n, line in self.logs(run, nxt):   # lines written between the two reads
                    if on_line:
                        on_line(line)
                if status["phase"] != "Succeeded":
                    raise RunFailed(status)
                return status
            if deadline is not None and time.monotonic() >= deadline:
                raise errors.OperationalError(f"run {run} still {status['phase']} after {timeout}s (left running)")
            time.sleep(poll)

    def run(self, job: str, on_line: Callable[[str], None] | None = None, poll: float = 2.0,
            timeout: float | None = None, cancel_on_error: bool = True, **params) -> dict:
        """submit + follow. Cancels the run if waiting is interrupted (Ctrl-C) or times out, unless
        cancel_on_error is False."""
        run = self.submit(job, **params)
        if on_line:
            on_line(f"run {run} submitted")
        try:
            return self.follow(run, on_line, poll, timeout)
        except (KeyboardInterrupt, errors.OperationalError):
            if cancel_on_error:
                try:
                    self.cancel(run)
                except errors.Error:
                    pass
            raise
