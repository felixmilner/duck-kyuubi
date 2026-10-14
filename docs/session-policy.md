# The session policy

A Kyuubi session conf advisor ([`SessionPolicy`](../kyuubi-session-policy/src/main/java/io/github/felixmilner/duckkyuubi/policy/SessionPolicy.java))
that decides what a session may ask for and routes it between the duckdb and launcher dialects. It runs in
the Kyuubi server when each session opens, and its overlay wins over anything the client sets.

```properties
kyuubi.session.conf.advisor=io.github.felixmilner.duckkyuubi.policy.SessionPolicy
```

Put `kyuubi-session-policy-<version>.jar` on the server's classpath (`$KYUUBI_HOME/jars`), not the engine's.

## What it does

- **Names a namespace for every user:** sets `spark.kubernetes.namespace` and `kyuubi.kubernetes.namespace`
  to `spark-<user>`, where the launcher runs that user's jobs (and Kyuubi would run their Spark engine).
  User names must fit in a namespace name: `[a-z]([a-z0-9-]{0,40}[a-z0-9])?`.
- **Allows only an allowlist of session config:** `kyuubi.engine.type` (`JDBC` or `SPARK_SQL`),
  `kyuubi.engine.jdbc.type` (`duckdb` or `launcher`), `kyuubi.session.name`, `spark.driver.memory`,
  `spark.driver.cores`, `spark.sql.*` and `spark.executor.*`, plus the keys Kyuubi's frontends and the Hive
  JDBC driver add themselves. Any other key fails the session ("session config ... isn't allowed").
- **Routes launcher sessions:** a session with `kyuubi.engine.jdbc.type=launcher` gets the JDBC engine, the
  `launcher` connection provider, an engine of its own (`kyuubi.engine.share.level.subdomain=launcher`) and
  a 384 MiB heap. Kyuubi picks a user's engine by share level, engine type, user and subdomain, not by
  dialect; without the subdomain, a launcher session would share the user's duckdb engine.

## Why an allowlist

A denylist leaks, because session config is powerful:

- `kyuubi.engine.jdbc.connection.url`, `...driver.class` and the like would point the engine at another
  database, or at a local DuckDB file inside the Kyuubi pod.
- `kyuubi.engine.*.java.options` start engine JVMs with options of the user's choosing, e.g.
  `-XX:OnError=<command>`, which runs in the Kyuubi pod.
- Kyuubi hands non-`spark.` keys to spark-submit with `spark.` prepended, so `kubernetes.namespace` becomes
  `spark.kubernetes.namespace`.

To allow more, edit the lists in `SessionPolicy` and rebuild. It's meant as a starting point.

## Authentication

Everything here, and in the launcher, keys off the Kyuubi user name, so the server must authenticate it.
The examples use `kyuubi.authentication=NONE` to get started quickly, which lets anyone who reaches the
ports claim any name. For anything shared, configure one of Kyuubi's authentication methods (JDBC against a
user table, LDAP, Kerberos, a custom provider) and put TLS in front of the REST API, since Basic
authentication sends the password with every request.
