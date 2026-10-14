"""A launcher job that runs DuckDB: the job contract (docs/launcher.md) in a few lines.

- Parameters arrive as arguments (the catalog's "arg") or environment variables (its "env").
- DUCKDB_MEMORY_LIMIT and DUCKDB_THREADS follow the pod's memory and cpu; KYUUBI_USER and
  LAUNCHER_RUN say who started the run and which run this is.
- Progress goes to stdout, which STATUS and LOGS read.
- The last line that is a JSON object is the run's summary (STATUS, the client, Airflow's XCom).
- The exit code is the result; the catalog's exit_codes explain the failures.
"""

import argparse
import json
import os
import sys
import time

import duckdb


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--millions", type=int, default=10, help="rows to generate, in millions")
    args = parser.parse_args()   # exits 2 on bad arguments

    con = duckdb.connect(config={
        "memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT", "1GB"),
        "threads": int(os.environ.get("DUCKDB_THREADS", "1")),
    })
    rows = args.millions * 1_000_000
    memory = con.sql("SELECT current_setting('memory_limit')").fetchone()[0]
    print(f"run {os.environ.get('LAUNCHER_RUN', '-')} for {os.environ.get('KYUUBI_USER', '-')}: "
          f"{rows:,} rows, DuckDB {duckdb.__version__}, memory_limit {memory}", flush=True)
    started = time.monotonic()
    groups = con.execute(
        "SELECT i % 7 AS k, count(*) AS n, sum(i) AS total FROM range(?) t(i) GROUP BY k ORDER BY k", [rows]
    ).fetchall()
    for k, n, total in groups:
        print(f"group {k}: {n:,} rows, sum {total:,}", flush=True)
    print(json.dumps({"rows": rows, "groups": len(groups), "seconds": round(time.monotonic() - started, 2)}),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
