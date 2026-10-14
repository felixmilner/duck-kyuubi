#!/usr/bin/env bash
# Apache Kyuubi on this machine with the duck-kyuubi plugins: REST on localhost:10099, Thrift on 10009.
# No Kubernetes and NO AUTHENTICATION (any user name, any password): for trying it out on a laptop.
#   - duckdb sessions (the default) run SQL on the Quack server from run-quack.sh
#   - launcher sessions answer SHOW JOBS from examples/jobs/catalog.json and check RUN requests, but
#     starting a job needs Kubernetes (examples/kubernetes)
# Needs Java 17+, Maven and network access on first run. Ctrl-C stops it.
set -euo pipefail
source "$(dirname "$0")/env.sh"

tgz=$WORK/apache-kyuubi-$KYUUBI_VERSION-bin.tgz
fetch "https://downloads.apache.org/kyuubi/kyuubi-$KYUUBI_VERSION/apache-kyuubi-$KYUUBI_VERSION-bin.tgz" "$tgz" 2>/dev/null \
  || fetch "https://archive.apache.org/dist/kyuubi/kyuubi-$KYUUBI_VERSION/apache-kyuubi-$KYUUBI_VERSION-bin.tgz" "$tgz"
KYUUBI_HOME=$WORK/apache-kyuubi-$KYUUBI_VERSION-bin
[[ -d $KYUUBI_HOME ]] || tar -xzf "$tgz" -C "$WORK"
duckdb_jar

if [[ -z ${SKIP_BUILD:-} ]]; then
  (cd "$ROOT" && mvn -q -B -DskipTests package)
fi
lib=$KYUUBI_HOME/externals/duck-kyuubi
rm -rf "$lib" && mkdir -p "$lib"
cp "$DUCKDB_JDBC_JAR" "$ROOT"/kyuubi-duckdb/target/kyuubi-duckdb-*.jar "$ROOT"/kyuubi-launcher/target/kyuubi-launcher-*.jar "$lib/"
rm -f "$KYUUBI_HOME"/jars/kyuubi-session-policy-*.jar
cp "$ROOT"/kyuubi-session-policy/target/kyuubi-session-policy-*.jar "$KYUUBI_HOME/jars/"

cat > "$KYUUBI_HOME/conf/kyuubi-defaults.conf" <<CONF
kyuubi.frontend.protocols=THRIFT_BINARY,REST
kyuubi.frontend.bind.host=localhost
kyuubi.frontend.thrift.binary.bind.port=10009
kyuubi.frontend.rest.bind.port=10099
# for trying it out only: anyone can connect as anyone
kyuubi.authentication=NONE

# the duckdb engine by default: the JDBC engine with the duckdb dialect, one engine per user
kyuubi.engine.type=JDBC
kyuubi.engine.share.level=USER
kyuubi.engine.jdbc.type=duckdb
kyuubi.engine.jdbc.driver.class=org.duckdb.DuckDBDriver
kyuubi.engine.jdbc.connection.url=jdbc:duckdb:
kyuubi.engine.jdbc.connection.provider=duckdb
kyuubi.engine.jdbc.extra.classpath=$lib/*

# config allowlist, and launcher sessions (kyuubi.engine.jdbc.type=launcher) on their own engine
kyuubi.session.conf.advisor=io.github.felixmilner.duckkyuubi.policy.SessionPolicy
CONF

# read by the engines, which inherit this environment
export QUACK_URI=quack:localhost:$QUACK_PORT
export LAUNCHER_CATALOG=${LAUNCHER_CATALOG:-$ROOT/examples/jobs/catalog.json}
echo "Kyuubi $KYUUBI_VERSION: REST http://localhost:10099, Thrift jdbc:hive2://localhost:10009/ (logs in $KYUUBI_HOME/logs)"
exec "$KYUUBI_HOME/bin/kyuubi" run
