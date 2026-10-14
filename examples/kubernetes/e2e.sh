#!/usr/bin/env bash
# The walkthrough in README.md as one script, then the client's live tests against the result: both
# shims end to end on a kind cluster (CI runs this). Needs docker, kind, kubectl, openssl, and Python
# with python/kyuubi-client and pytest installed. Extra arguments go to pytest.
#   KIND_CLUSTER=duck-kyuubi examples/kubernetes/e2e.sh     # creates the cluster if it doesn't exist
set -euo pipefail
cd "$(dirname "$0")/../.."
cluster=${KIND_CLUSTER:-duck-kyuubi}
ns=duck-kyuubi

kind get clusters 2>/dev/null | grep -qx "$cluster" || kind create cluster --name "$cluster"
docker build -f docker/kyuubi.Dockerfile -t duck-kyuubi/kyuubi:0.1.0 .
docker build -t duck-kyuubi/quack-server:0.1.0 quack-server
docker build -t duck-kyuubi/duckdb-job:0.1.0 examples/jobs/duckdb-job
kind load docker-image --name "$cluster" \
  duck-kyuubi/kyuubi:0.1.0 duck-kyuubi/quack-server:0.1.0 duck-kyuubi/duckdb-job:0.1.0

kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f -
kubectl -n $ns get secret quack-token >/dev/null 2>&1 \
  || kubectl -n $ns create secret generic quack-token --from-literal=TOKEN="$(openssl rand -hex 24)"
for cm in "quack-server --from-file=server.sql=quack-server/server.sql" \
          "kyuubi-jobs --from-file=catalog.json=examples/jobs/catalog.json"; do
  kubectl -n $ns create configmap $cm --dry-run=client -o yaml | kubectl apply -f -
done
kubectl apply -f examples/kubernetes/quack-server.yaml -f examples/kubernetes/kyuubi.yaml
kubectl -n $ns rollout status deployment/quack-server --timeout=5m
kubectl -n $ns rollout status deployment/kyuubi --timeout=5m
examples/kubernetes/add-user.sh alice

kubectl -n $ns port-forward svc/kyuubi 10099:10099 >/dev/null 2>&1 &
forward=$!
trap 'kill $forward 2>/dev/null || true' EXIT
# -f: wait for a 2xx; the REST API answers 500 for a moment while Kyuubi's metrics start
for _ in $(seq 60); do
  curl -sf -o /dev/null http://localhost:10099/api/v1/ping && break
  sleep 1
done
curl -sf -o /dev/null http://localhost:10099/api/v1/ping || { echo "Kyuubi's REST API isn't answering" >&2; exit 1; }

export KYUUBI_URL=http://localhost:10099 KYUUBI_USER=alice KYUUBI_PASSWORD=
# everything but Spark (not deployed) and login checks (authentication is off in this example)
python -m pytest python/kyuubi-client --live -m "not spark and not auth" "$@"
python examples/python/query_duckdb.py
python examples/python/run_job.py duckdb-demo millions=5
