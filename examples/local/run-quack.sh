#!/usr/bin/env bash
# A DuckDB Quack server on localhost:9494 for the duckdb engine, running quack-server/server.sql
# (a demo table). Ctrl-C stops it. Needs Java 17+ and network access on first run (the DuckDB jar,
# then the quack extension).
set -euo pipefail
source "$(dirname "$0")/env.sh"
duckdb_jar
mkdir -p "$WORK/quack-classes"
javac -d "$WORK/quack-classes" "$ROOT/quack-server/QuackServer.java"
sql=$WORK/server.sql
sed "s/quack:0.0.0.0:9494/quack:0.0.0.0:$QUACK_PORT/" "$ROOT/quack-server/server.sql" > "$sql"
echo "Quack server on port $QUACK_PORT (token from \$QUACK_TOKEN)"
TOKEN=$QUACK_TOKEN exec java -cp "$WORK/quack-classes:$DUCKDB_JDBC_JAR" QuackServer "$sql"
