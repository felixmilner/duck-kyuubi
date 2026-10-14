# Shared settings for run-quack.sh and run-kyuubi.sh; every one can be overridden from the environment.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WORK=${WORK:-$ROOT/.local}                         # downloads and the unpacked Kyuubi (git-ignored)
KYUUBI_VERSION=${KYUUBI_VERSION:-1.12.0}
# Quack needs a DuckDB 2.0 build; those are only on DuckDB's staging Maven repository so far.
# The Kyuubi engine and the Quack server must run the same DuckDB build.
DUCKDB_JDBC_VERSION=${DUCKDB_JDBC_VERSION:-2.0.0-alpha43385-881}
DUCKDB_JDBC_JAR=${DUCKDB_JDBC_JAR:-$WORK/duckdb_jdbc-$DUCKDB_JDBC_VERSION.jar}
QUACK_PORT=${QUACK_PORT:-9494}
export QUACK_TOKEN=${QUACK_TOKEN:-local-dev-token}   # what both sides share; pick your own outside a laptop

fetch() {   # fetch URL FILE: download once
  [[ -s $2 ]] && return
  mkdir -p "$(dirname "$2")"
  echo "downloading $1" >&2
  curl -fsSL --retry 3 -o "$2.part" "$1" && mv "$2.part" "$2"
}

duckdb_jar() {
  fetch "https://duckdb-staging.duckdb.org/duckdb/duckdb-java/maven/org/duckdb/duckdb_jdbc/$DUCKDB_JDBC_VERSION/duckdb_jdbc-$DUCKDB_JDBC_VERSION.jar" \
    "$DUCKDB_JDBC_JAR"
}
