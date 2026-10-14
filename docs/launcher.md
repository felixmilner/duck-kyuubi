# The launcher (`kyuubi.engine.jdbc.type=launcher`)

A Kyuubi session that asks for the `launcher` dialect becomes a job runner. Instead of SQL it takes a
handful of commands: run a job from an admin-defined catalog as a Kubernetes pod in the user's
namespace, follow it, cancel it. Kyuubi stays the one front door for batch jobs as well as queries:
same login, same REST and Thrift APIs, the user's own namespace, quota and credentials.

Source: [`kyuubi-launcher`](../kyuubi-launcher/src/main/java/io/github/felixmilner/duckkyuubi/launcher/).
It is a JDBC-engine dialect plus connection provider, so it needs no Kyuubi fork.

## Statements

| Statement | Returns |
|---|---|
| `SHOW JOBS` | `job, description, image, params` (the parameters as JSON) |
| `RUN <job> [name=value ...]` | starts the job and holds the statement until it ends: `run, job, status, exit_code, seconds, summary, log_tail` (last 15 lines). A failed run is an error carrying the exit code, the catalog's meaning of it and the last 25 log lines |
| `RUN <job> async=true [name=value ...]` | starts the job and returns at once: `run, job, status` (`Submitted`) |
| `STATUS <run>` | `run, job, phase, exit_code, seconds, summary, message`. `phase` is Pending, Running, Succeeded, Failed or Terminating; `exit_code` is -1 until the run ends; `message` is why a pod is waiting, or the catalog's meaning of a failure's exit code |
| `SHOW RUNS` | `run, job, phase, started` for the user's runs |
| `LOGS <run> [from=N]` | `n, line` from line N (default 1), at most 2,000 lines per call, from at most 4 MB of log |
| `CANCEL <run>` | deletes the run's pod: `run, status` (`cancelled`) |

Keywords are case-insensitive and a trailing `;` is fine. Job names match `[a-z0-9-]+`, parameter names
`[a-z_]+`, and values `[A-Za-z0-9_.,:/=+-]{1,200}`: no spaces or quotes, so nothing a user passes can
become shell or SQL. Anything else is refused ("launcher sessions accept only: ..."). Cancelling a
statement that is waiting on a `RUN` (Ctrl-C in the client, Kyuubi's cancel) deletes the pod too.

Clients follow a run with `async=true` plus `STATUS` and `LOGS`. That is what the Python `Launcher`, the
`kyuubi job` CLI and the Airflow operator do, so no request stays open for the length of a job. A plain
`RUN` copies the pod's log into the engine's own log file while it waits, but Kyuubi's JDBC engine keeps
no operation log, so clients don't see those lines.

## The catalog

A JSON file, `$LAUNCHER_CATALOG` (default `/opt/kyuubi/conf/jobs/catalog.json`), which the launcher
re-reads on every statement: edits apply as soon as the file changes (a ConfigMap volume refreshes within
about a minute). It is trusted configuration: it decides which images run, with what command and which
Secrets. [examples/jobs/catalog.json](../examples/jobs/catalog.json) has two jobs.

```json
{
  "hello": {
    "description": "shown by SHOW JOBS",
    "image": "cgr.dev/chainguard/python:latest",
    "image_pull_policy": "IfNotPresent",
    "command": ["python", "-u", "-c", "..."],
    "args": ["--fixed", "argument"],
    "secrets": ["hello-credentials"],
    "timeout_s": 900,
    "exit_codes": {"4": "another run holds the lock"},
    "params": {
      "seconds": {"type": "int", "default": "5", "min": 1, "max": 600, "arg": "--seconds"},
      "memory":  {"type": "size", "default": "128Mi", "min": "64Mi", "max": "1Gi", "to": "resources"}
    }
  }
}
```

| Field | Meaning |
|---|---|
| `image` | the container image |
| `image_pull_policy` | default `Always` (so a rebuilt image that reuses its tag is picked up); `IfNotPresent` for images preloaded on the nodes |
| `command` | optional; replaces the image's entrypoint |
| `args` | fixed arguments; the parameters' arguments follow them |
| `secrets` | Secrets in the user's namespace whose keys become environment variables (`envFrom`) |
| `timeout_s` | the pod's `activeDeadlineSeconds` (default 3600) |
| `exit_codes` | what each non-zero exit code means, shown with a failed run |
| `params` | what users may set: anything else is refused |

Each parameter:

| Field | Meaning |
|---|---|
| `type` | `string` (default), `int`, `cores`, `size` (`512Mi`, `2Gi`: Ki, Mi or Gi) or `enum` |
| `default` | used when the user doesn't give it; without one, the parameter is simply left out |
| `min`, `max` | bounds for `int`, `cores` (numbers) and `size` (quantities) |
| `values` | the allowed values of an `enum` |
| `map` | replaces a value after it's checked, e.g. `{"any": ""}` |
| `to` | where the value goes: `arg` (the default; appended as `<arg> <value>` if the parameter has an `arg`), `env` (as the variable named by `env`), `node` (a `kubernetes.io/hostname` node selector; empty means none), `timeout` (replaces `timeout_s`) or `resources` |

A `cores` parameter sets the pod's CPU limit; its CPU request stays 500m so a large job still fits beside
system pods. A `size` parameter sets the pod's memory request and limit. Without them a run gets 1 CPU and
1 GiB. `int` values are checked as numbers, so `2.5` passes an `int` check: give a range that makes sense
either way.

## The job contract

What a job image can rely on and should do. [examples/jobs/duckdb-job](../examples/jobs/duckdb-job/job.py)
is a working example.

- **Input:** the catalog's `args` and the parameters' arguments; the Secrets' keys and the `env`
  parameters as environment variables. Always set: `KYUUBI_USER` (who started the run), `LAUNCHER_RUN`
  (the run id), and, for jobs that run DuckDB, `DUCKDB_MEMORY_LIMIT` (3/4 of the pod's memory, e.g.
  `768MB`) and `DUCKDB_THREADS` (the CPU limit rounded up), unless a parameter sets them.
- **Output:** stdout and stderr are the run's log. Flush as you go (`python -u`) so `LOGS` shows progress.
- **Summary:** the last log line that is a JSON object (starts with `{` and ends with `}`, within the
  last 200 lines) is the run's summary: in `STATUS`, the client's result, and Airflow's XCom.
- **Result:** the exit code. Explain the non-zero ones in the catalog's `exit_codes`.
- **User:** the pod runs with `runAsNonRoot`, so the image's user must be non-root and numeric
  (`USER 10001`); an image that runs as root fails to start (`CreateContainerConfigError`).

## The pod

```yaml
metadata:
  name: <job>-<8 hex digits>          # the run id
  namespace: spark-<user>
  labels: {app.kubernetes.io/managed-by: kyuubi-launcher, duck-kyuubi/job: <job>, duck-kyuubi/user: <user>}
spec:
  restartPolicy: Never
  automountServiceAccountToken: false   # jobs get no Kubernetes API access
  activeDeadlineSeconds: <timeout>
  containers:
    - securityContext: {allowPrivilegeEscalation: false, runAsNonRoot: true, capabilities: {drop: [ALL]}}
      resources: {requests: {cpu: 500m, memory: <memory>}, limits: {cpu: <cpu>, memory: <memory>}}
```

While a plain `RUN` waits, it polls every 3 seconds. A pod that is still unschedulable after a minute, or
can't pull or start its image, is deleted and reported. Waiting stops 2 minutes past the timeout. Every
`RUN` first deletes the user's finished runs that are more than a day old; until then `STATUS` and `LOGS`
work on them.

## Kubernetes setup

- Kyuubi runs in the cluster (its JDBC engines are child JVMs in the Kyuubi pod). The launcher calls the
  API server at `kubernetes.default.svc` with the pod's service-account token, read fresh for each call.
- Each user has a namespace `spark-<user>`. In it, a Role lets Kyuubi's service account `create`, `get`,
  `list` and `delete` pods and `get` `pods/log`, and nothing else; Kyuubi's account has no other rights.
  The Secrets that user's jobs name go there too.
- A `ResourceQuota` and `LimitRange` in each namespace cap what one user can run at once.
- [examples/kubernetes/add-user.sh](../examples/kubernetes/add-user.sh) creates all of that, with Pod
  Security `baseline` on the namespace.

## Security model

- **Who:** the run belongs to the Kyuubi user of the session, so Kyuubi's authentication is the whole
  basis: with `kyuubi.authentication=NONE` anyone can start jobs as anyone.
- **What:** users choose a catalog job and the parameters it declares, within its limits. The image,
  command, Secrets and security context come from the catalog.
- **Where:** pods run in the user's namespace, under its quota and Pod Security, with only the Secrets
  the job names, no service-account token, non-root and without capabilities.
- **The engine:** a launcher session's engine runs in the Kyuubi pod, which holds Kyuubi's service-account
  token and the server's other credentials. So it runs no user SQL: answers are built as `VALUES` queries
  from values it quotes itself, on an in-memory DuckDB with external access, extension loading and
  configuration changes switched off (`LauncherConnectionProvider`).
- **Separate engines:** every user has engines of their own (`kyuubi.engine.share.level=USER`), and the
  session policy gives launcher sessions an engine separate from the user's duckdb engine
  ([session-policy.md](session-policy.md)).
