# duck-kyuubi

Two plugins that make [Apache Kyuubi](https://kyuubi.apache.org/) a single gateway for DuckDB queries and
for containerised batch jobs. Users get one login and one REST/Thrift endpoint for both, and each user's
own engines, namespace, quota and credentials.

- **duckdb** ([`kyuubi-duckdb`](kyuubi-duckdb)): runs each session's SQL on a shared DuckDB server over
  DuckDB's Quack protocol, keeping DuckDB's compute out of the Kyuubi pod. [docs/duckdb.md](docs/duckdb.md)
- **launcher** ([`kyuubi-launcher`](kyuubi-launcher)): turns a session into a job runner. `RUN <job>` starts
  a job from an admin-defined catalog as a pod in the user's Kubernetes namespace, and `STATUS`, `LOGS` and
  `CANCEL` follow it. Good for Python jobs that run DuckDB, or any image. [docs/launcher.md](docs/launcher.md)

Around them:

- **a session policy** ([`kyuubi-session-policy`](kyuubi-session-policy)): a session-config allowlist, a
  namespace per user, and routing between the two dialects. [docs/session-policy.md](docs/session-policy.md)
- **a Python client** ([`python/kyuubi-client`](python/kyuubi-client)): DB-API 2.0, a `kyuubi` command line
  and a `Launcher` class, over Kyuubi's REST API.
- **an Airflow operator** ([`python/airflow-kyuubi`](python/airflow-kyuubi)) that runs launcher jobs.

No Kyuubi fork: Kyuubi's engine types are a fixed list, but its JDBC engine loads dialects and connection
providers with Java's ServiceLoader from `kyuubi.engine.jdbc.extra.classpath`, so both shims are jars you
add to a stock Kyuubi.

```
 beeline / JDBC / REST / kyuubi CLI / Airflow
        │ Thrift :10009, REST :10099
 Kyuubi server ── session policy: allowlist, namespace spark-<user>, routing
   ├─ JDBC engine, dialect duckdb ─── Quack ───────────▶ Quack server (DuckDB): tables, Iceberg catalog
   └─ JDBC engine, dialect launcher ─ Kubernetes API ──▶ job pods in spark-<user>
```

## Compatibility

| | |
|---|---|
| Kyuubi | 1.12.0. The dialect API is internal to Kyuubi, so build against the Kyuubi you run (`-Dkyuubi.version=...`) and retest when you upgrade |
| Java | 17+ |
| DuckDB | duckdb needs DuckDB 2.0 for Quack: alpha builds from DuckDB's staging Maven repository so far, the same build on the engine and the server (`2.0.0-alpha43385-881` in the original deployment). The launcher works with any DuckDB JDBC driver, e.g. `org.duckdb:duckdb_jdbc` from Maven Central |
| Kubernetes | the launcher, and Kyuubi running in the cluster |

These plugins come from a deployment on a small k3s cluster, where both ran end to end with Kyuubi 1.12.0
and DuckDB 2.0.0-alpha43385. In this repository, the unit tests cover the plugins and clients, and a local
Kyuubi 1.12.0 has run them (session policy, launcher catalog and refusals, duckdb up to the Quack
connection). The Dockerfiles and Kubernetes manifests are adapted from that deployment and haven't been
run in this form.

## Build

```bash
mvn package           # kyuubi-duckdb, kyuubi-launcher and kyuubi-session-policy jars, under */target
pip install -e python/kyuubi-client
```

## Install into Kyuubi

1. Copy `kyuubi-duckdb-*.jar`, `kyuubi-launcher-*.jar` and DuckDB's JDBC driver to a directory for the
   JDBC engine, e.g. `$KYUUBI_HOME/externals/duck-kyuubi/`. Copy `kyuubi-session-policy-*.jar` to
   `$KYUUBI_HOME/jars/` (the server's classpath).
2. In `kyuubi-defaults.conf`:

   ```properties
   kyuubi.engine.type=JDBC
   kyuubi.engine.share.level=USER
   kyuubi.engine.jdbc.type=duckdb
   kyuubi.engine.jdbc.driver.class=org.duckdb.DuckDBDriver
   kyuubi.engine.jdbc.connection.url=jdbc:duckdb:
   kyuubi.engine.jdbc.connection.provider=duckdb
   kyuubi.engine.jdbc.extra.classpath=/opt/kyuubi/externals/duck-kyuubi/*
   kyuubi.session.conf.advisor=io.github.felixmilner.duckkyuubi.policy.SessionPolicy
   ```

3. In the Kyuubi server's environment (its engines inherit it): `QUACK_URI` and `QUACK_TOKEN` for the
   duckdb dialect, `LAUNCHER_CATALOG` for the launcher's job catalog.

Sessions get duckdb by default; a session with `kyuubi.engine.jdbc.type=launcher` gets the launcher.
[docker/kyuubi.Dockerfile](docker/kyuubi.Dockerfile) builds an image laid out this way.

## Try it

**On a laptop, without Kubernetes** ([examples/local](examples/local)): a Quack server and Kyuubi, both
local. The duckdb dialect runs your SQL on the local Quack server; the launcher lists its catalog and checks
requests, but starting a job needs Kubernetes.

```bash
examples/local/run-quack.sh        # terminal 1: Quack server on :9494 with a demo table
examples/local/run-kyuubi.sh       # terminal 2: Kyuubi 1.12.0 on :10009 (Thrift) and :10099 (REST), no auth
export KYUUBI_URL=http://localhost:10099 KYUUBI_USER=alice KYUUBI_PASSWORD=
kyuubi sql "SELECT fizz, count(*) FROM demo GROUP BY fizz"
kyuubi job list
```

**On Kubernetes** ([examples/kubernetes](examples/kubernetes/README.md)): both shims on a kind cluster:
images, manifests, a user namespace, then SQL and jobs.

## Examples

| | |
|---|---|
| [examples/python/query_duckdb.py](examples/python/query_duckdb.py) | SQL on the duckdb dialect with the DB-API client |
| [examples/python/run_job.py](examples/python/run_job.py) | list the catalog, run a job, follow its log, print its summary |
| [examples/sql](examples/sql) | the same from beeline |
| [examples/jobs/catalog.json](examples/jobs/catalog.json) | a job catalog: `hello` (log lines, a summary, a chosen exit code) and `duckdb-demo` |
| [examples/jobs/duckdb-job](examples/jobs/duckdb-job) | a Python job image that runs DuckDB, following the job contract |
| [python/airflow-kyuubi/examples/dag_jobs.py](python/airflow-kyuubi/examples/dag_jobs.py) | jobs from Airflow, including a mapped fan-out |

## Security, briefly

Both shims key everything off the Kyuubi user name, so configure Kyuubi authentication for anything
shared; the examples run without it. The duckdb dialect's server runs every user's SQL in one process with
one set of credentials: lock it down as [quack-server/server.sql](quack-server/server.sql) does, and use one
server per user or group where users must not see each other's data. The launcher runs only catalog jobs,
in the user's namespace, as non-root pods without a service-account token. More in the docs.

## Tests

```bash
mvn verify                                          # the plugins, the launcher against a fake Kubernetes API
pytest python/kyuubi-client python/airflow-kyuubi   # the clients (Airflow's tests need apache-airflow)
examples/local/smoke-test.sh                        # a local Kyuubi 1.12.0 with the plugins, and the live tests
                                                    # that need no Quack server or Kubernetes
examples/kubernetes/e2e.sh                          # everything on kind: images, manifests, Quack, job pods
```

CI runs all four. `pytest python/kyuubi-client --live` runs the live tests against any gateway at
`$KYUUBI_URL` ([tests/conftest.py](python/kyuubi-client/tests/conftest.py)).
