-- The launcher from beeline. The session asks for the launcher in the JDBC URL:
--   beeline -u 'jdbc:hive2://localhost:10009/;#kyuubi.engine.jdbc.type=launcher' -n alice -f examples/sql/launcher.sql
-- RUN without async=true holds the statement until the job ends and returns its result.
SHOW JOBS;
RUN hello name=beeline seconds=3;
RUN hello seconds=60 async=true;
SHOW RUNS;
-- then, with a run id from the line above:
--   STATUS hello-1a2b3c4d;  LOGS hello-1a2b3c4d from=1;  CANCEL hello-1a2b3c4d;
