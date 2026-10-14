-- SQL on the duckdb engine with Kyuubi's beeline (bin/beeline in the Kyuubi distribution):
--   beeline -u 'jdbc:hive2://localhost:10009/' -n alice -f examples/sql/duckdb.sql
-- Each statement runs on the Quack server; `demo` comes from quack-server/server.sql.
SELECT version();
SELECT fizz, count(*) AS n FROM demo GROUP BY fizz ORDER BY fizz;
-- one Kyuubi session is one DuckDB session on the server: temp tables carry over
CREATE TEMP TABLE mine AS SELECT * FROM demo WHERE id < 10;
SELECT * FROM mine ORDER BY id;
