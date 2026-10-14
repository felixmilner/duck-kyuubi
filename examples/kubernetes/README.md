# Both shims on Kubernetes

Kyuubi with the duckdb and launcher dialects, a Quack server, and a user namespace for jobs, on any
cluster. [e2e.sh](e2e.sh) does all of this on kind and then runs the client's live tests; CI runs it. The commands use [kind](https://kind.sigs.k8s.io/) and images built locally; on another cluster,
push the images to a registry and change the `image:` lines (and the catalog's `duckdb-demo` image).

These manifests are adapted from a deployment that ran on k3s with Kyuubi 1.12.0 and DuckDB
2.0.0-alpha43385; they haven't been run in exactly this form. Authentication is off
(`kyuubi.authentication=NONE`), which is fine on a laptop only: [docs/session-policy.md](../../docs/session-policy.md).

## 1. Cluster and images

From the repository root:

```bash
kind create cluster --name duck-kyuubi

docker build -f docker/kyuubi.Dockerfile -t duck-kyuubi/kyuubi:0.1.0 .
docker build -t duck-kyuubi/quack-server:0.1.0 quack-server
docker build -t duck-kyuubi/duckdb-job:0.1.0 examples/jobs/duckdb-job
kind load docker-image --name duck-kyuubi \
  duck-kyuubi/kyuubi:0.1.0 duck-kyuubi/quack-server:0.1.0 duck-kyuubi/duckdb-job:0.1.0
```

The `hello` job pulls `cgr.dev/chainguard/python`, a Python image that runs as a non-root user (the
launcher requires one: [docs/launcher.md](../../docs/launcher.md#the-job-contract)).

## 2. Kyuubi and the Quack server

```bash
kubectl create namespace duck-kyuubi
# the token the Kyuubi engines use to reach the Quack server
kubectl -n duck-kyuubi create secret generic quack-token --from-literal=TOKEN="$(openssl rand -hex 24)"
kubectl -n duck-kyuubi create configmap quack-server --from-file=server.sql=quack-server/server.sql
kubectl -n duck-kyuubi create configmap kyuubi-jobs --from-file=catalog.json=examples/jobs/catalog.json

kubectl apply -f examples/kubernetes/quack-server.yaml -f examples/kubernetes/kyuubi.yaml
kubectl -n duck-kyuubi rollout status deployment/quack-server deployment/kyuubi
```

To change the catalog later: `kubectl -n duck-kyuubi create configmap kyuubi-jobs --from-file=catalog.json=...
--dry-run=client -o yaml | kubectl apply -f -`. The launcher picks it up within a minute, without a restart.

## 3. A user

```bash
examples/kubernetes/add-user.sh alice    # namespace spark-alice: Role for Kyuubi, quota, Pod Security
```

## 4. Use it

```bash
kubectl -n duck-kyuubi port-forward svc/kyuubi 10009 10099 &
export KYUUBI_URL=http://localhost:10099 KYUUBI_USER=alice KYUUBI_PASSWORD=
pip install -e python/kyuubi-client
```

SQL on the Quack server (the duckdb dialect, every session's default):

```bash
kyuubi sql "SELECT fizz, count(*) FROM demo GROUP BY fizz"
python examples/python/query_duckdb.py
```

Jobs (the launcher):

```bash
kyuubi job list
kyuubi job run hello seconds=5 name=alice          # follows the log; exits with the job's exit code
kyuubi job run duckdb-demo millions=50 memory=2Gi
python examples/python/run_job.py hello exit=4      # a failure: exit code 4 and the catalog's explanation
kubectl -n spark-alice get pods                     # the runs, kept for a day
```

Or with beeline from the Kyuubi distribution, through the Thrift port:
[examples/sql/duckdb.sql](../sql/duckdb.sql), [examples/sql/launcher.sql](../sql/launcher.sql).

The client's live tests run against the same gateway:

```bash
pytest python/kyuubi-client --live -m "not spark and not auth"
```

## Clean up

```bash
kind delete cluster --name duck-kyuubi
```
