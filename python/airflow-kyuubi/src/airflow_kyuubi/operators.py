"""KyuubiJobOperator: run a catalog job through the gateway's launcher engine and wait for it.

    KyuubiJobOperator(task_id="hello", job="hello",
                      job_params={"seconds": "10", "memory": "256Mi"})

The task submits the job (`RUN <job> async=true ...`), follows its log into the task log, and returns the
job's run summary (the last line of its log that is a JSON object) as the task's XCom. A job that fails
fails the task with its exit code and the catalog's explanation of it; `skip_on_exit_codes` turns chosen
codes into a skipped task instead (e.g. a job that exits 4 when another run already holds its lock).
Killing the task cancels the run. Whether a retry is safe is up to the job.

The login is an Airflow connection (default id `kyuubi_default`): host = the Kyuubi URL, login and password,
and in Extra {"verify": "/path/to/ca.crt"} (or false). Each task runs as that Kyuubi user, so its pods
land in that user's namespace under that user's quota, with the Secrets the catalog gives the job.

Mapped tasks work as for any operator (`job_params` is templated), which is how a pipeline is sharded.
(The argument is not called `params`: Airflow reserves that name and wraps its values in Param objects.)

    KyuubiJobOperator.partial(task_id="ingest", job="my-sharded-job").expand(
        job_params=[{"shard": "0", "shards": "2"}, {"shard": "1", "shards": "2"}])
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

try:   # Airflow 3
    from airflow.sdk import BaseOperator
    from airflow.sdk.bases.hook import BaseHook
    from airflow.sdk.exceptions import AirflowException, AirflowSkipException
except ImportError:   # Airflow 2
    from airflow.exceptions import AirflowException, AirflowSkipException
    from airflow.hooks.base import BaseHook
    from airflow.models import BaseOperator

from kyuubi_client import KyuubiClient, Launcher, RunFailed
from kyuubi_client import errors


class KyuubiJobOperator(BaseOperator):
    template_fields: Sequence[str] = ("job", "job_params")
    ui_color = "#e8f1fb"

    def __init__(
        self,
        *,
        job: str,
        job_params: dict[str, Any] | None = None,
        kyuubi_conn_id: str = "kyuubi_default",
        poll_interval: float = 5.0,
        run_timeout: float | None = None,
        skip_on_exit_codes: Sequence[int] = (),
        cancel_on_kill: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job = job
        self.job_params = job_params or {}
        self.kyuubi_conn_id = kyuubi_conn_id
        self.poll_interval = poll_interval
        self.run_timeout = run_timeout
        self.skip_on_exit_codes = tuple(skip_on_exit_codes)
        self.cancel_on_kill = cancel_on_kill
        self._launcher: Launcher | None = None
        self._run: str | None = None

    # -- connection
    def _client(self) -> KyuubiClient:
        conn = BaseHook.get_connection(self.kyuubi_conn_id)
        extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}
        host = conn.host or ""
        url = host if "://" in host else f"{conn.schema or 'https'}://{host}" + (f":{conn.port}" if conn.port else "")
        return KyuubiClient(url, conn.login or "", conn.password or "", verify=extra.get("verify", True))

    # -- the task
    def execute(self, context: Any) -> dict:
        with self._client() as client, Launcher(client) as launcher:
            self._launcher = launcher
            self._run = launcher.submit(self.job, **{k: str(v) for k, v in self.job_params.items()})
            self.log.info("Kyuubi run %s of job %s submitted", self._run, self.job)
            ti = context.get("ti") if isinstance(context, dict) else None
            if ti is not None:
                ti.xcom_push(key="kyuubi_run", value=self._run)
            try:
                status = launcher.follow(self._run, self.log.info, self.poll_interval, self.run_timeout)
            except RunFailed as e:
                code = e.status.get("exit_code")
                if code in self.skip_on_exit_codes:
                    raise AirflowSkipException(f"{e} (skipped: exit code {code})") from None
                raise AirflowException(str(e)) from None
            except errors.OperationalError:
                self._cancel()
                raise
            self.log.info("Kyuubi run %s %s in %s s", self._run, status["phase"], status["seconds"])
            try:
                return json.loads(status.get("summary") or "{}")
            except ValueError:
                return {"summary": status.get("summary")}

    def on_kill(self) -> None:
        if self.cancel_on_kill:
            self._cancel()

    def _cancel(self) -> None:
        if self._launcher is not None and self._run is not None:
            try:
                self._launcher.cancel(self._run)
                self.log.info("Kyuubi run %s cancelled", self._run)
            except errors.Error as e:
                self.log.warning("could not cancel Kyuubi run %s: %s", self._run, e)

