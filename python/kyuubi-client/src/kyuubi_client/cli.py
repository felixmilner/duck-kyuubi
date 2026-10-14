"""The `kyuubi` command: SQL and PySpark against the gateway from a terminal.

    kyuubi sql "SELECT 42 AS answer"
    kyuubi sql --engine spark -f models.sql --format csv
    kyuubi python "spark.range(10).count()"
    kyuubi shell [--engine spark] [--python]
    kyuubi sessions [--close ID ...]
    kyuubi job list | run JOB [name=value ...] [--async] | runs | status RUN | logs RUN [--follow] | cancel RUN

Settings come from flags, then $KYUUBI_URL, $KYUUBI_USER, $KYUUBI_CA and $KYUUBI_ENGINE, then a
profile in ~/.config/kyuubi/config.ini (or $KYUUBI_CONFIG; --profile or $KYUUBI_PROFILE picks the
section, default "default"):

    [default]
    url = https://kyuubi.example.com
    ca = /path/to/your-ca.crt

The password never goes in the profile: it's $KYUUBI_PASSWORD, or you're asked (credentials.py).
"""

import argparse
import codeop
import configparser
import csv
import getpass
import json
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from . import errors
from .client import ENGINES, TERMINAL, KyuubiClient, Operation, Session
from .credentials import lookup_password
from .launcher import Launcher, RunFailed

DEFAULT_URL = "http://localhost:10099"
SETTINGS = ("url", "user", "ca", "engine")
SPARK_NOTICE = "(waiting for the Spark engine; about a minute if it isn't running yet)"


def load_settings(args) -> dict:
    path = Path(os.environ.get("KYUUBI_CONFIG") or Path.home() / ".config" / "kyuubi" / "config.ini")
    name = args.profile or os.environ.get("KYUUBI_PROFILE")
    parser = configparser.ConfigParser()
    profile = {}
    if parser.read(path):
        if name and name not in parser:
            raise errors.InterfaceError(f"no profile [{name}] in {path}")
        profile = dict(parser[name or "default"]) if (name or "default") in parser else {}
        if "password" in profile:
            raise errors.InterfaceError(f"{path}: keep the password out of the profile (use KYUUBI_PASSWORD)")
        if unknown := set(profile) - set(SETTINGS):
            raise errors.InterfaceError(f"{path}: unknown setting {', '.join(sorted(unknown))}")
    elif name:
        raise errors.InterfaceError(f"no profile file at {path}")
    cfg = {
        key: getattr(args, key, None) or os.environ.get(f"KYUUBI_{key.upper()}") or profile.get(key)
        for key in SETTINGS
    }
    cfg["url"] = cfg["url"] or DEFAULT_URL
    cfg["user"] = cfg["user"] or getpass.getuser()
    if cfg["engine"] and cfg["engine"] not in ENGINES:
        raise errors.InterfaceError(f"unknown engine {cfg['engine']!r}: use {' or '.join(ENGINES)}")
    return cfg


def connect(cfg: dict) -> KyuubiClient:
    return KyuubiClient(cfg["url"], cfg["user"], lookup_password(cfg["user"]), verify=cfg["ca"] or True)


def session_conf(pairs: list[str]) -> dict:
    conf = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep:
            raise errors.InterfaceError(f"--conf {pair!r}: expected key=value")
        conf[key] = value
    return conf


def split_sql(text: str, backslash: bool = False) -> tuple[list[str], str]:
    """Splits on semicolons outside quotes and comments. Returns the complete statements and the
    unterminated rest ("" if that's only whitespace and comments). backslash: Spark escapes
    characters in strings with a backslash; DuckDB doesn't."""
    statements, start, code, quote = [], 0, False, None
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if quote:
            if backslash and c == "\\":
                i += 1
            elif c == quote:   # a doubled quote closes and reopens: same result
                quote = None
        elif text.startswith("--", i):
            end = text.find("\n", i)
            i = n if end < 0 else end
            continue
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            if end < 0:   # unfinished comment: keep it for the next line
                return statements, text[start:]
            i = end + 2
            continue
        elif c == ";":
            if code:
                statements.append(text[start:i].strip())
            start, code = i + 1, False
        elif not c.isspace():
            code = True
            if c in "'\"`":
                quote = c
        i += 1
    return statements, text[start:] if code else ""


def wait(op: Operation, timeout: float | None, notice: str | None = None) -> Operation:
    """op.wait(), printing `notice` to stderr if the statement is still running after 3 s."""
    if notice and sys.stderr.isatty():
        try:
            for _ in range(15):
                if op.status() in TERMINAL:
                    break
                time.sleep(0.2)
            else:
                print(notice, file=sys.stderr)
        except KeyboardInterrupt:
            op.cancel()
            raise
    return op.wait(timeout)


def text(value, table: bool = False) -> str:
    if value is None:
        return "NULL" if table else ""
    if isinstance(value, bool):
        return "true" if value else "false"
    value = str(value)
    return value.replace("\n", "\\n") if table else value


def print_rows(op: Operation, fmt: str, out=None):
    out = out or sys.stdout
    if not op.columns:
        return
    names = [d[0] for d in op.description]
    if fmt == "csv":
        writer = csv.writer(out)
        writer.writerow(names)
        for row in op.rows():
            writer.writerow([text(v) for v in row])
    elif fmt == "json":   # JSON Lines, one object per row
        for row in op.rows():
            print(json.dumps(dict(zip(names, row)), default=str, ensure_ascii=False), file=out)
    else:
        rows = list(op.rows())
        cells = [[text(v, table=True) for v in row] for row in rows]
        widths = [max([len(name)] + [len(r[i]) for r in cells]) for i, name in enumerate(names)]
        numeric = [bool(rows) and all(isinstance(r[i], (int, float, Decimal)) and not isinstance(r[i], bool)
                                      for r in rows if r[i] is not None) for i in range(len(names))]
        print(" | ".join(n.ljust(w) for n, w in zip(names, widths)).rstrip(), file=out)
        print("-+-".join("-" * w for w in widths), file=out)
        for row in cells:
            print(" | ".join(c.rjust(w) if num else c.ljust(w)
                             for c, w, num in zip(row, widths, numeric)).rstrip(), file=out)
        print(f"({len(rows)} row{'' if len(rows) == 1 else 's'})", file=out)


def print_python(op: Operation, out=None):
    """A Python statement's result is one row: output, status, ename, evalue, traceback."""
    out = out or sys.stdout
    for row in op.fetch():
        if row[0]:
            print(row[0].rstrip("\n"), file=out)


def read_code(args) -> str:
    if args.file:
        return sys.stdin.read() if args.file == "-" else Path(args.file).read_text()
    if args.code is not None:
        return args.code
    if not sys.stdin.isatty():
        return sys.stdin.read()
    raise errors.InterfaceError("nothing to run: pass the code, -f FILE, or pipe it in")


def run_sql(args, cfg: dict):
    statements, rest = split_sql(read_code(args), backslash=cfg["engine"] == "spark")
    if rest.strip():
        statements.append(rest.strip())
    with connect(cfg) as client, client.open_session(cfg["engine"], session_conf(args.conf)) as session:
        notice = SPARK_NOTICE if cfg["engine"] == "spark" else None
        for statement in statements:
            op = wait(session.execute(statement, wait=False), args.timeout, notice)
            notice = None
            with op:
                print_rows(op, args.format)


def run_python(args, cfg: dict):
    if cfg["engine"] not in (None, "spark"):
        raise errors.InterfaceError("Python runs on the Spark engine only")
    with connect(cfg) as client, client.open_session("spark", session_conf(args.conf)) as session:
        op = session.execute(read_code(args), language="python", wait=False)
        with wait(op, args.timeout, SPARK_NOTICE) as op:
            print_python(op)


def list_sessions(args, cfg: dict):
    with connect(cfg) as client:
        for identifier in args.close or []:
            Session(client, identifier, None).close()
        if args.close:
            return
        # GET /sessions lists every user's sessions; show this user's
        mine = [s for s in client.request("GET", "/sessions") if s.get("user") == client.user]
        engines = {conf["kyuubi.engine.type"]: name for name, conf in ENGINES.items()}
        print(f"{'id':36}  {'engine':6}  {'created':19}  {'idle':>7}  ops  name")
        for s in sorted(mine, key=lambda s: s.get("createTime", 0)):
            engine = engines.get((s.get("conf") or {}).get("kyuubi.engine.type"), "?")
            created = datetime.fromtimestamp(s.get("createTime", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S")
            idle = f"{s.get('idleTime', 0) // 1000}s"
            print(f"{s['identifier']:36}  {engine:6}  {created}  {idle:>7}  {s.get('totalOperations', 0):>3}"
                  f"  {s.get('sessionName') or ''}".rstrip())


SHELL_HELP = """\
SQL statements end with ';'. Python statements run when complete (a blank line ends a block).
  \\python   switch to Python (Spark engine)    \\sql   switch to SQL
  \\q        quit (or Ctrl-D)                   Ctrl-C cancels the running statement"""


def shell(args, cfg: dict):
    engine = cfg["engine"]
    if args.python and engine not in (None, "spark"):
        raise errors.InterfaceError("Python runs on the Spark engine only")
    engine = engine or ("spark" if args.python else None)
    language = "python" if args.python else "sql"
    history = Path.home() / ".kyuubi_history"
    try:
        import readline
        try:
            readline.read_history_file(history)
        except OSError:
            pass
    except ImportError:
        readline = None
    with connect(cfg) as client, client.open_session(engine, session_conf(args.conf)) as session:
        label = engine or "duckdb"
        print(f"{client.user} @ {cfg['url']}, {label} engine. \\? for help.")
        notice = SPARK_NOTICE if engine == "spark" else None
        buffer = ""
        while True:
            prompt = f"{label}:{'py' if language == 'python' else 'sql'}> "
            try:
                line = input(" " * (len(prompt) - 2) + "> " if buffer else prompt)
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
                buffer = ""
                continue
            if not buffer and line.strip().startswith("\\"):
                command = line.strip()
                if command in ("\\q", "\\quit"):
                    break
                if command == "\\python":
                    if engine != "spark":
                        print("Python needs the Spark engine: kyuubi shell --engine spark", file=sys.stderr)
                    else:
                        language = "python"
                elif command == "\\sql":
                    language = "sql"
                else:
                    print(SHELL_HELP)
                continue
            buffer += line + "\n"
            if language == "python":
                try:
                    complete = codeop.compile_command(buffer, symbol="single") is not None
                except (SyntaxError, ValueError, OverflowError):
                    complete = True   # let the engine report it
                if not complete and line.strip():
                    continue
                statements, buffer = ([buffer.rstrip("\n")] if buffer.strip() else []), ""
            else:
                statements, buffer = split_sql(buffer, backslash=engine == "spark")
            for statement in statements:
                try:
                    op = session.execute(statement, language=language, wait=False)
                    with wait(op, args.timeout, notice) as op:
                        notice = None
                        if language == "python":
                            print_python(op)
                        else:
                            print_rows(op, "table")
                except KeyboardInterrupt:
                    print("cancelled", file=sys.stderr)
                    buffer = ""
                    break
                except errors.Error as e:
                    print(f"error: {e}", file=sys.stderr)
                    buffer = ""   # don't run the rest of what was typed after a failure
                    break
    if readline:
        try:
            readline.write_history_file(history)
        except OSError:
            pass


def run_job(args, cfg: dict):
    """kyuubi job ...: batch jobs from the gateway's catalog, run as pods in your namespace."""
    def table(rows: list[dict]):
        if not rows:
            print("(none)")
            return
        names = list(rows[0])
        cells = [[text(r[n], table=True) for n in names] for r in rows]
        widths = [max([len(n)] + [len(c[i]) for c in cells]) for i, n in enumerate(names)]
        print(" | ".join(n.ljust(w) for n, w in zip(names, widths)).rstrip())
        print("-+-".join("-" * w for w in widths))
        for c in cells:
            print(" | ".join(v.ljust(w) for v, w in zip(c, widths)).rstrip())

    with connect(cfg) as client, Launcher(client) as jobs:
        action = args.action
        if action == "list":
            table(jobs.list())
        elif action == "runs":
            table(jobs.runs())
        elif action == "status":
            table([jobs.status(args.target)])
        elif action == "cancel":
            jobs.cancel(args.target)
            print(f"{args.target} cancelled")
        elif action == "logs":
            if args.follow:
                try:
                    jobs.follow(args.target, print)
                except RunFailed as e:
                    print(f"error: {e}", file=sys.stderr)
                    raise SystemExit(e.status.get("exit_code") or 1)
            else:
                for _, line in jobs.logs(args.target, args.from_line):
                    print(line)
        else:   # run
            params = {}
            for pair in args.params:
                key, sep, value = pair.partition("=")
                if not sep:
                    raise errors.InterfaceError(f"parameter {pair!r}: expected name=value")
                params[key] = value
            if args.detach:
                print(jobs.submit(args.target, **params))
                return
            try:
                status = jobs.run(args.target, on_line=lambda line: print(line, flush=True),
                                  timeout=args.timeout, **params)
            except RunFailed as e:
                print(f"error: {e}", file=sys.stderr)
                raise SystemExit(e.status.get("exit_code") if e.status.get("exit_code", 0) > 0 else 1)
            print(f"-- {status['run']} {status['phase']} in {status['seconds']} s", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", help=f"Kyuubi URL (default {DEFAULT_URL})")
    common.add_argument("--user", help="login (default: your OS user)")
    common.add_argument("--ca", help="CA file for an https URL")
    common.add_argument("--profile", help="section of the profile file")

    running = argparse.ArgumentParser(add_help=False)
    running.add_argument("--conf", action="append", metavar="KEY=VALUE",
                         help="session config, e.g. spark.executor.memory=2g (repeatable)")
    running.add_argument("--timeout", type=float, help="per-statement limit in seconds (then cancel)")

    parser = argparse.ArgumentParser(prog="kyuubi", description="Kyuubi gateway client.")
    commands = parser.add_subparsers(dest="command", required=True)

    sql = commands.add_parser("sql", parents=[common, running], help="run SQL statements")
    sql.add_argument("code", nargs="?", help="statements, separated by ';'")
    sql.add_argument("-f", "--file", help="read the statements from FILE ('-' for stdin)")
    sql.add_argument("--engine", choices=list(ENGINES), help="default: the server's (duckdb)")
    sql.add_argument("--format", choices=("table", "csv", "json"), default="table")
    sql.set_defaults(run=run_sql)

    python = commands.add_parser("python", parents=[common, running],
                                 help="run PySpark code in your Spark engine")
    python.add_argument("code", nargs="?")
    python.add_argument("-f", "--file", help="read the code from FILE ('-' for stdin)")
    python.set_defaults(run=run_python)

    sh = commands.add_parser("shell", parents=[common, running], help="interactive SQL/Python")
    sh.add_argument("--engine", choices=list(ENGINES), help="default: the server's (duckdb)")
    sh.add_argument("--python", action="store_true", help="start in Python (implies --engine spark)")
    sh.set_defaults(run=shell)

    job = commands.add_parser("job", parents=[common], help="batch jobs from the gateway's catalog (launcher)")
    job_actions = job.add_subparsers(dest="action", required=True)
    job_actions.add_parser("list", help="the job catalog")
    job_actions.add_parser("runs", help="your runs")
    jr = job_actions.add_parser("run", help="start a job, follow its log, exit with its exit code")
    jr.add_argument("target", metavar="JOB")
    jr.add_argument("params", nargs="*", metavar="name=value", help="the job's declared parameters")
    jr.add_argument("--detach", "--async", action="store_true", help="print the run id and return")
    jr.add_argument("--timeout", type=float, help="stop waiting after this many seconds and cancel the run")
    for name, text_ in (("status", "a run's phase, exit code and summary"), ("cancel", "delete a run's pod")):
        job_actions.add_parser(name, help=text_).add_argument("target", metavar="RUN")
    jl = job_actions.add_parser("logs", help="a run's log")
    jl.add_argument("target", metavar="RUN")
    jl.add_argument("--follow", "-f", action="store_true", help="keep printing until the run ends")
    jl.add_argument("--from", dest="from_line", type=int, default=1, metavar="N", help="start at line N")
    job.set_defaults(run=run_job)

    sessions = commands.add_parser("sessions", parents=[common], help="list or close your sessions")
    sessions.add_argument("--close", nargs="+", metavar="ID")
    sessions.set_defaults(run=list_sessions)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.run(args, load_settings(args))
    except KeyboardInterrupt:
        print("cancelled", file=sys.stderr)
        return 130
    except (errors.Error, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
