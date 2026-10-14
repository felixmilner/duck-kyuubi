# kyuubi-client

Python client for Apache Kyuubi's REST API (v1), with the duck-kyuubi launcher. Needs only `requests`.

- `KyuubiClient` / `Session` / `Operation`: the REST resources. Open a session on an engine, run
  SQL, wait, page through results, read logs, cancel.
- `connect()`: a PEP 249 (DB-API 2.0) connection on top, for dbt and other Python tools.
- `Launcher`: jobs through the launcher engine ([docs/launcher.md](../../docs/launcher.md)).
- `kyuubi`: a command line for all of the above.

```
pip install -e python/kyuubi-client      # from the repository root
```

## SQL

```python
from kyuubi_client import connect

with connect("http://localhost:10099", user="alice", password="", engine="duckdb") as conn:
    cur = conn.cursor()
    cur.execute("SELECT * FROM demo WHERE id < %s", (10,))
    print(cur.description, cur.fetchall())
```

`engine` is `"duckdb"` (Kyuubi's JDBC engine, whose default dialect the server sets), `"spark"`, or
`None` for the server's default. For https, pass `verify="/path/to/ca.crt"` (or `False`). A wrong
password raises `HTTPError` ("login failed", status 401/403).

Lower level, e.g. to run without waiting or follow an operation's log:

```python
from kyuubi_client import KyuubiClient

with KyuubiClient("http://localhost:10099", user="alice") as client, \
        client.open_session(engine="duckdb") as session:
    op = session.execute("SELECT count(*) FROM demo", wait=False)
    op.wait(timeout=600)
    print(op.fetch())
```

On a Spark engine, `session.execute(code, language="python")` runs PySpark in the engine (`spark` is its
session). It returns one row: `output` (the printed output or the value's repr), `status`, then `ename`,
`evalue`, `traceback`. If it raises, `QueryError.message` is its traceback.

Notes:

- A session is one engine connection: temp tables, `USE` and `BEGIN`/`COMMIT` carry over between
  statements. Kyuubi starts the engine on the session's first statement.
- Parameters (`%s`, `%(name)s`) are quoted into the SQL client-side: the REST API has no bind
  parameters. Quoting follows the session's engine (Spark escapes backslashes, DuckDB doesn't).
- Results: DECIMAL, DATE and TIMESTAMP become `Decimal`, `date`, `datetime`; numbers and booleans
  come back as such. Everything else is the engine's text, including DuckDB lists, structs and
  timestamps with time zone (the duckdb dialect reports them as strings), and Spark's
  ARRAY/MAP/STRUCT (JSON text).
- Metadata calls (`session.tables()` and the like) describe the duckdb engine's local, empty DuckDB,
  not the Quack server; query `information_schema` instead.
- Errors: a failed statement raises `QueryError` (a `DatabaseError`) with the engine's message;
  `.raw` has Kyuubi's full text. Connection and HTTP problems raise `OperationalError`.
- `commit()` is a no-op and `rollback()` raises `NotSupportedError`: statements autocommit unless
  you run `BEGIN` yourself.

## Jobs

```python
from kyuubi_client import KyuubiClient, Launcher, RunFailed

with KyuubiClient("http://localhost:10099", user="alice") as client, Launcher(client) as jobs:
    print(jobs.list())                                       # the catalog
    status = jobs.run("hello", on_line=print, seconds="5")   # submit, follow the log, wait
    print(status["summary"])                                 # the job's last JSON line
    run = jobs.submit("hello", seconds="60")                 # or start it and come back later
    jobs.status(run); jobs.logs(run, from_line=1); jobs.cancel(run)
```

`run` raises `RunFailed` (with `.status`) when the job fails or is cancelled. It submits with
`async=true` and follows the run by polling `STATUS` and `LOGS`, so no request stays open for the length
of the job and a lost connection loses nothing: `follow(run)` picks it up again.

## CLI

```
export KYUUBI_URL=http://localhost:10099 KYUUBI_USER=alice KYUUBI_PASSWORD=
kyuubi sql "SELECT * FROM demo LIMIT 5"                  # table output
kyuubi sql -f query.sql --format csv                      # several statements, ';'-separated
kyuubi shell                                              # one session; \python, \sql, \q
kyuubi sessions [--close ID ...]                          # your open sessions

kyuubi job list                                           # the catalog and each job's parameters
kyuubi job run hello seconds=5 name=me                    # follows the log, exits with the job's code
kyuubi job run hello seconds=600 --detach                 # prints the run id and returns
kyuubi job runs | status RUN | logs RUN [--follow] [--from N] | cancel RUN
```

Settings come from flags (`--url`, `--user`, `--ca`, `--engine`, given right after the command: `kyuubi
job --url ... list`), then `KYUUBI_URL`, `KYUUBI_USER`, `KYUUBI_CA`, `KYUUBI_ENGINE`, then a profile file,
`~/.config/kyuubi/config.ini` (or `$KYUUBI_CONFIG`; `--profile` picks a section). The default URL is
`http://localhost:10099`.

```ini
[default]
url = https://kyuubi.example.com
ca = /path/to/ca.crt
```

The password is `$KYUUBI_PASSWORD` (empty is fine for a server without authentication), else you're
asked; never the profile (the CLI refuses a `password` key). `--conf key=value` sets session config
within the server's allowlist, and `--timeout` cancels a statement after that many seconds. Ctrl-C
cancels the running statement. Errors go to stderr with exit code 1.

## Test

```
pytest python/kyuubi-client                     # offline
KYUUBI_URL=http://localhost:10099 pytest python/kyuubi-client --live -m "not k8s and not spark and not auth"
```

`--live` also runs the tests against a running Kyuubi; [tests/conftest.py](tests/conftest.py) lists the
markers for what each needs (a Quack server, Kubernetes, Spark, a server that checks passwords).
