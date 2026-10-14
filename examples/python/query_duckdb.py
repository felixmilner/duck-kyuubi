"""SQL on the duckdb engine through Kyuubi's REST API, with the DB-API client (python/kyuubi-client).

    pip install -e python/kyuubi-client
    KYUUBI_URL=http://localhost:10099 KYUUBI_USER=alice KYUUBI_PASSWORD= python examples/python/query_duckdb.py

Every statement runs on the Quack server; `demo` is the table quack-server/server.sql creates.
"""

import os

from kyuubi_client import connect

url = os.environ.get("KYUUBI_URL", "http://localhost:10099")
user = os.environ.get("KYUUBI_USER", "alice")
password = os.environ.get("KYUUBI_PASSWORD", "")

# engine="duckdb" asks for the JDBC engine; the server's default dialect (kyuubi.engine.jdbc.type) is duckdb
with connect(url, user=user, password=password, engine="duckdb", timeout=300) as conn:
    cur = conn.cursor()
    cur.execute("SELECT version() AS duckdb_version")
    print("DuckDB on the Quack server:", cur.fetchone()[0])

    # parameters are quoted client-side (paramstyle pyformat)
    cur.execute("SELECT fizz, count(*) AS n, min(label) AS first FROM demo WHERE id < %s GROUP BY fizz ORDER BY fizz",
                (100,))
    print([d[0] for d in cur.description])
    for row in cur.fetchall():
        print(row)

    # one Kyuubi session is one server-side DuckDB session: temp tables and variables carry over
    cur.execute("CREATE TEMP TABLE mine AS SELECT * FROM demo WHERE fizz")
    cur.execute("SELECT count(*) FROM mine")
    print("rows in my temp table:", cur.fetchone()[0])
