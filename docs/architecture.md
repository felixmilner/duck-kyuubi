# Architecture

How the pieces fit together: who talks to whom, where each piece runs, which configuration each one reads,
and what happens on a query and on a job. GitHub renders the diagrams; anything that renders Mermaid will.

## The touch points

```mermaid
flowchart LR
  subgraph clients["Clients"]
    beeline["beeline / JDBC / BI tools"]
    py["Python: kyuubi-client<br/>DB-API, kyuubi CLI, Launcher"]
    af["Airflow: KyuubiJobOperator"]
  end

  subgraph kyuubi["Kyuubi pod"]
    direction TB
    server["Kyuubi server<br/>Thrift :10009, REST :10099<br/>authentication"]
    policy["SessionPolicy<br/>allowlist, spark-&lt;user&gt;, routing"]
    subgraph engines["JDBC engines: child JVMs, one per user and kind"]
      duck["duckdb engine<br/>DuckDBDialect + DuckDBConnectionProvider<br/>embedded DuckDB, Quack client"]
      launch["launcher engine<br/>LauncherDialect + LauncherConnectionProvider<br/>locked-down DuckDB, Kube client"]
    end
    server --- policy
    server -- "Thrift" --> duck
    server -- "Thrift" --> launch
  end

  quack["Quack server<br/>DuckDB, server.sql"]
  k8s["Kubernetes API"]
  subgraph userns["namespace spark-&lt;user&gt;"]
    pods["job pods<br/>from the catalog"]
  end
  storage[("tables, Iceberg catalog,<br/>object storage")]

  beeline -- "Thrift" --> server
  py -- "REST" --> server
  af -- "REST" --> server
  duck -- "Quack: whole statements" --> quack
  launch -- "create, get, list, delete pods; read logs" --> k8s
  k8s --> pods
  quack --> storage
  pods -. "jobs read and write data" .-> storage
```

| Piece | Runs | What it does | Source |
|---|---|---|---|
| Clients | anywhere | beeline and JDBC over Thrift; the Python client, `kyuubi` CLI and Airflow operator over REST | [python](../python) |
| Kyuubi server | Kyuubi pod | logins, sessions, the REST and Thrift APIs; starts and finds engines | Apache Kyuubi 1.12.0 |
| Session policy | in the server | checks each session's config, names the user's namespace, routes launcher sessions | [kyuubi-session-policy](../kyuubi-session-policy) |
| duckdb engine | a JVM in the Kyuubi pod, per user | Kyuubi's JDBC engine with the duckdb dialect: ships each statement to the Quack server | [kyuubi-duckdb](../kyuubi-duckdb), [docs/duckdb.md](duckdb.md) |
| launcher engine | a JVM in the Kyuubi pod, per user | Kyuubi's JDBC engine with the launcher dialect: catalog jobs as pods | [kyuubi-launcher](../kyuubi-launcher), [docs/launcher.md](launcher.md) |
| Quack server | its own pod | DuckDB serving every user's SQL over Quack | [quack-server](../quack-server) |
| Job pods | the user's namespace | one per run; any image, following the job contract | [examples/jobs](../examples/jobs) |

## Configuration touch points

| What | Read by | Set in the examples | Controls |
|---|---|---|---|
| `kyuubi-defaults.conf` | the server, which passes it on to engines | ConfigMap `kyuubi`, or `examples/local/run-kyuubi.sh` | default engine and dialect, the engines' classpath, the session policy, authentication |
| Session config | the session policy, then the engine | JDBC URL (`;#key=value`), REST `configs`, `kyuubi --conf` | engine type, dialect (`duckdb` or `launcher`), Spark sizing; nothing outside the allowlist |
| `QUACK_URI`, `QUACK_TOKEN` | the duckdb engine (inherited from the server's environment) | Deployment `kyuubi`, Secret `quack-token` | which Quack server, and its token |
| `LAUNCHER_CATALOG` (`catalog.json`) | the launcher engine, on every statement | ConfigMap `kyuubi-jobs` | the jobs: images, commands, Secrets, parameters and their limits |
| `server.sql`, `TOKEN` | the Quack server, at start | ConfigMap `quack-server`, Secret `quack-token` | the data or catalog it serves, its lockdown, the token clients need |
| Namespace `spark-<user>` | Kubernetes | `examples/kubernetes/add-user.sh` | what Kyuubi may do there (a Role), quotas, Pod Security, the Secrets jobs get |
| `KYUUBI_USER`, `LAUNCHER_RUN`, `DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS` | job containers | set by the launcher on each run | who started the run, which run it is, DuckDB's sizing |

## Where it runs on Kubernetes

```mermaid
flowchart TB
  subgraph dk["namespace duck-kyuubi"]
    direction LR
    conf[/"ConfigMap kyuubi: kyuubi-defaults.conf<br/>ConfigMap kyuubi-jobs: catalog.json"/]
    tok[/"Secret quack-token"/]
    kpod["Deployment kyuubi<br/>server JVM + engine JVMs<br/>ServiceAccount kyuubi: no rights here"]
    qpod["Deployment quack-server<br/>Service :9494"]
    sql[/"ConfigMap quack-server: server.sql"/]
    conf --> kpod
    tok -- "QUACK_TOKEN" --> kpod
    kpod -- "Quack" --> qpod
    tok -- "TOKEN" --> qpod
    sql --> qpod
  end

  subgraph alice["namespace spark-alice"]
    direction LR
    arole["Role kyuubi: pods create, get, list, delete;<br/>pods/log get. Bound to ServiceAccount kyuubi"]
    aguard["ResourceQuota, LimitRange,<br/>Pod Security baseline"]
    asecrets[/"Secrets alice's jobs name"/]
    ajobs["alice's job pods<br/>non-root, no capabilities,<br/>no service-account token"]
    asecrets -- "envFrom" --> ajobs
  end

  subgraph bob["namespace spark-bob"]
    direction LR
    brole["the same Role, quota and limits"]
    bsecrets[/"Secrets bob's jobs name"/]
    bjobs["bob's job pods"]
    bsecrets -- "envFrom" --> bjobs
  end

  dk -- "the launcher calls the Kubernetes API as ServiceAccount kyuubi" --> alice
  dk -- "add-user.sh creates one namespace per Kyuubi user" --> bob
```

Kyuubi's service account has no rights in its own namespace and only what the Role grants in each user's
namespace. Job pods get no service-account token at all. The JDBC engines aren't pods: Kyuubi's JDBC engine
has no Kubernetes deploy mode, so they run as child JVMs in the Kyuubi pod, which is why they only relay.

## Which engine a session gets

```mermaid
flowchart LR
  s1["alice opens a session<br/>(server default)"] --> p
  s2["alice: kyuubi.engine.jdbc.type=launcher"] --> p
  s3["bob opens a session"] --> p
  s4["anyone: a key outside the allowlist<br/>e.g. kyuubi.engine.jdbc.connection.url"] --> p
  p{"SessionPolicy"}
  p -- "jdbc.type duckdb" --> e1["engine USER / JDBC / alice / default<br/>duckdb dialect, 1 GiB heap"]
  p -- "provider launcher, subdomain launcher, 384m" --> e2["engine USER / JDBC / alice / launcher<br/>launcher dialect"]
  p --> e3["engine USER / JDBC / bob / default<br/>duckdb dialect"]
  p -- "refused" --> x["session fails:<br/>isn't allowed"]
```

Kyuubi keys engines by share level, engine type, user and subdomain, not by dialect, so the session policy
gives launcher sessions a subdomain of their own. Each user's sessions of one kind share an engine; users
never share one (`kyuubi.engine.share.level=USER`).

## A query, step by step

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant K as Kyuubi server
  participant P as SessionPolicy
  participant E as duckdb engine (JVM)
  participant Q as Quack server
  C->>K: open session (alice, conf)
  K->>P: getConfOverlay(alice, conf)
  P-->>K: namespaces, or refuse
  K->>E: find alice's engine, or start one
  E->>E: DuckDBConnectionProvider: LOAD quack, CREATE SECRET quack, ATTACH QUACK_URI AS remote
  C->>K: SELECT fizz, count(*) FROM demo GROUP BY fizz
  K->>E: execute
  E->>Q: SELECT * FROM quack_query_by_name('remote', 'SELECT fizz, ...')
  Q->>Q: runs it in alice's server-side session
  Q-->>E: result rows only
  E-->>K: rows (TINYINT, BLOB converted)
  C->>K: fetch
  K-->>C: rows
```

Each Kyuubi session is one DuckDB session on the server, so temp tables and transactions carry over between
its statements. The engine's own DuckDB holds no data.

## A job, step by step

```mermaid
sequenceDiagram
  autonumber
  participant C as Client (Launcher, CLI, Airflow)
  participant K as Kyuubi server
  participant L as launcher engine (JVM)
  participant A as Kubernetes API
  participant J as job pod in spark-alice
  C->>K: open session, kyuubi.engine.jdbc.type=launcher
  K->>L: SessionPolicy routes it to alice's launcher engine
  C->>K: RUN hello async=true seconds=5
  K->>L: execute
  L->>L: check the job and parameters against catalog.json
  L->>A: delete alice's runs that finished over a day ago
  L->>A: POST pod (image, args, Secrets, limits from the catalog)
  A->>J: schedule and start
  L-->>C: run hello-1a2b3c4d, Submitted (through Kyuubi)
  loop until the run ends
    Note over J: runs, writing its log
    C->>K: STATUS hello-1a2b3c4d, LOGS hello-1a2b3c4d from=N
    K->>L: execute
    L->>A: GET pod, GET pod log
    L-->>C: phase, the new log lines
  end
  Note over J: last log line: a JSON summary, then exit 0
  C->>K: STATUS hello-1a2b3c4d
  L-->>C: Succeeded, exit code 0, summary
```

The client polls, so no request stays open for the length of a job; `RUN` without `async=true` instead holds
the statement until the pod ends.

## What crosses each boundary

| From → to | Carries | Limited by |
|---|---|---|
| Client → Kyuubi | user name and password, SQL or launcher commands, session config | Kyuubi's authentication; the session policy's allowlist |
| Kyuubi → engine | the statement, the session's config | the policy's overlay (it wins over the client's) |
| duckdb engine → Quack server | whole statements, the token | the token; the server's lockdown (no local files, locked settings) |
| launcher engine → Kubernetes API | pods built from the catalog, with Kyuubi's service-account token | the Role in `spark-<user>`; the catalog; the parameters' limits |
| Job pod → the rest | whatever its image does, with the Secrets the catalog names | the namespace's quota and Pod Security; no service-account token |
