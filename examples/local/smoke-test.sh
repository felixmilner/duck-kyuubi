#!/usr/bin/env bash
# Kyuubi on this machine with the plugins (run-kyuubi.sh), using the stable DuckDB from Maven Central, then
# the client's live tests that need no Quack server and no Kubernetes: the session policy, the launcher's
# catalog and refusals, and the client against a real Kyuubi. CI runs this; extra arguments go to pytest.
# Needs Java 17+, Maven, Python with python/kyuubi-client and pytest installed, and ports 10009 and 10099.
set -euo pipefail
source "$(dirname "$0")/env.sh"
KYUUBI_HOME=$WORK/apache-kyuubi-$KYUUBI_VERSION-bin
version=${DUCKDB_TEST_VERSION:-1.5.6.0}
jar=$WORK/duckdb_jdbc-$version.jar
fetch "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/$version/duckdb_jdbc-$version.jar" "$jar"

log=$WORK/kyuubi-smoke.log
DUCKDB_JDBC_JAR=$jar "$ROOT/examples/local/run-kyuubi.sh" > "$log" 2>&1 &
# the server and the engines it started, which outlive it
trap 'pkill -f "$KYUUBI_HOME/" || true' EXIT
# -f: wait for a 2xx; the REST API answers 500 for a moment while Kyuubi's metrics start
for _ in $(seq 120); do
  curl -sf -o /dev/null "http://localhost:10099/api/v1/ping" && break
  sleep 1
done
curl -sf -o /dev/null "http://localhost:10099/api/v1/ping" || { echo "Kyuubi didn't start:" >&2; tail -50 "$log" >&2; exit 1; }

export KYUUBI_URL=http://localhost:10099 KYUUBI_USER=${KYUUBI_USER:-alice} KYUUBI_PASSWORD=
python -m pytest "$ROOT/python/kyuubi-client" --live -m "not quack and not k8s and not spark and not auth" "$@" \
  || { echo "--- Kyuubi's log: $log, $KYUUBI_HOME/logs, $KYUUBI_HOME/work/$KYUUBI_USER" >&2; exit 1; }
kyuubi job list
