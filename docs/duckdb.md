# The duckdb dialect (`kyuubi.engine.jdbc.type=duckdb`)

Runs every Kyuubi session's SQL on a shared DuckDB server, over DuckDB's Quack protocol. DuckDB's compute
stays out of the Kyuubi pod: Kyuubi's JDBC engine has no Kubernetes deploy mode (only `local` and `yarn`),
so its engine runs as a child JVM in the Kyuubi pod and should only relay.

Source: [`kyuubi-duckdb`](../kyuubi-duckdb/src/main/java/io/github/felixmilner/duckkyuubi/duckdb/) and
[`quack-server`](../quack-server/).

```
client ── Kyuubi session ──▶ JDBC engine (child JVM in the Kyuubi pod, one per user)
                               embedded, empty DuckDB with the Quack server ATTACHed as "remote"
                               every statement → SELECT * FROM quack_query_by_name('remote', '<statement>')
                                   │ Quack protocol, token
                             Quack server (DuckDB): your tables, or an attached Iceberg REST catalog
```

## How it works

There is no network JDBC driver for Quack: the only Quack client is DuckDB itself. So the engine's
"connection" is an embedded in-memory DuckDB that attaches the server, and the dialect sends the whole
statement there:

1. **Per Kyuubi session** (`DuckDBConnectionProvider`): `INSTALL quack; LOAD quack`, then
   `CREATE SECRET quack (TYPE quack, TOKEN '...')` from `$QUACK_TOKEN`, then
   `ATTACH '<QUACK_URI>' AS remote (DISABLE_SSL true)`. The token is in a secret, so it never appears in a
   statement Kyuubi sees or logs; if creating the secret fails, the error isn't chained, since its message
   could echo the statement.
2. **Per statement** (`DuckDBDialect`): `SELECT * FROM quack_query_by_name('remote', '<statement>')`, with
   quotes doubled and trailing semicolons removed. The server runs it and only the result rows come back.
3. **Per value:** Kyuubi's generic row generator gets TINYINT as a Short, and BLOBs in DuckDB's text form
   (`\x00\xFFhi`), which it can't do with what DuckDB's driver returns.

One Kyuubi session is one server-side DuckDB session, so temp tables, `USE`, `SET VARIABLE` and
`BEGIN`/`COMMIT`/`ROLLBACK` carry over between its statements, and sessions don't see each other's temp
tables.

## Configuration

`kyuubi-defaults.conf`:

```properties
kyuubi.engine.type=JDBC
kyuubi.engine.jdbc.type=duckdb
kyuubi.engine.jdbc.driver.class=org.duckdb.DuckDBDriver
kyuubi.engine.jdbc.connection.url=jdbc:duckdb:
kyuubi.engine.jdbc.connection.provider=duckdb
# kyuubi-duckdb-<version>.jar and DuckDB's JDBC driver (the same DuckDB build as the server)
kyuubi.engine.jdbc.extra.classpath=/opt/kyuubi/externals/duck-kyuubi/*
kyuubi.engine.share.level=USER
```

The Kyuubi server's environment, which its engines inherit:

| Variable | |
|---|---|
| `QUACK_URI` | the server, e.g. `quack:quack-server.duck-kyuubi.svc.cluster.local:9494` (default `quack:localhost:9494`) |
| `QUACK_TOKEN` | the server's token; required |
| `HOME` | where DuckDB looks for installed extensions (`$HOME/.duckdb/extensions`). Bake `quack` into the image ([docker/kyuubi.Dockerfile](../docker/kyuubi.Dockerfile) does) so the engine never downloads it |

## The Quack server

[quack-server/QuackServer.java](../quack-server/QuackServer.java) runs a SQL file on the pinned DuckDB JDBC
build and keeps the connection open, which keeps `quack_serve` running. Embedded DuckDB has no `getenv()`,
so it replaces `getenv('NAME')` in the file with that environment variable's value first, and redacts those
values from any error. It does that everywhere in the file, comments included, and stops if a variable isn't
set. [quack-server/server.sql](../quack-server/server.sql) is the demo:

- `LOAD quack`, then your data (a `demo` table).
- `SET disabled_filesystems = 'LocalFileSystem'` and `SET lock_configuration = true`. Clients' SQL runs in
  this process: without these, any Kyuubi user could read local files, `/proc/self/environ` with the
  server's credentials included, or change its settings. S3 and HTTP stay available.
- `SELECT count(*) FROM quack_serve('quack:0.0.0.0:9494', token := getenv('TOKEN'), allow_other_hostname
  := true, disable_ssl := true)`. `quack_serve` returns the token as a column; `count(*)` keeps it out of
  the log.

### An Iceberg REST catalog instead

What the original deployment ran (Apache Polaris; any Iceberg REST catalog that vends credentials works the
same way):

```sql
INSTALL quack; LOAD quack; LOAD iceberg; LOAD httpfs; LOAD avro;
CREATE SECRET catalog (TYPE iceberg, CLIENT_ID getenv('CLIENT_ID'), CLIENT_SECRET getenv('CLIENT_SECRET'),
    OAUTH2_SERVER_URI 'http://polaris:8181/api/catalog/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL');
-- No S3 secret: DuckDB uses the storage credentials the catalog vends per table.
ATTACH 'warehouse' AS warehouse (TYPE iceberg, SECRET catalog, URI 'http://polaris:8181/api/catalog',
    TABLE_RESOLUTION 'eager');
SET disabled_filesystems = 'LocalFileSystem';
SET lock_configuration = true;
SELECT count(*) AS quack_started FROM quack_serve('quack:0.0.0.0:9494', token := getenv('TOKEN'),
                                                  allow_other_hostname := true, disable_ssl := true);
```

- `TABLE_RESOLUTION 'eager'` loads each table's columns when the catalog is listed. The default lists
  tables with a placeholder `UNKNOWN` type until first read, which breaks the client's `ATTACH`
  ([duckdb-quack#202](https://github.com/duckdb/duckdb-quack/issues/202)).
- Users write fully qualified names (`warehouse.<namespace>.<table>`).
- DuckDB-Iceberg has no `CREATE OR REPLACE` (use `DROP` + `CREATE`). Commits are optimistic: when two
  commits to one table race, the catalog refuses the stale one (HTTP 409), DuckDB doesn't retry, and the
  statement fails while the session stays usable. Clients that write one table from several sessions
  should retry.

## Versions

Quack needs DuckDB 2.0. Its builds are alphas published only on DuckDB's staging Maven repository
(`https://duckdb-staging.duckdb.org/duckdb/duckdb-java/maven/`), and the engine and the server must run the
same build: the original deployment ran `2.0.0-alpha43385-881` on both. Pin it, and the extension version
that goes with it (`v2.0.0-alpha43385`), in both Dockerfiles. Expect to revisit this once DuckDB 2.0 is
released.

## Limitations

- **Whole statements only.** Binding `remote.<schema>.<table>` names through the attachment doesn't work on
  the 2.0 alphas ([duckdb-quack#190](https://github.com/duckdb/duckdb-quack/issues/190)), which is why the
  dialect ships each statement whole. Statements can't mix server tables with engine-local ones.
- **Metadata calls** (JDBC `getTables` and the like, and the REST API's catalogs/schemas/tables calls)
  describe the engine's local, empty DuckDB, not the server. `information_schema` and `DESCRIBE` work.
- **One shared server.** All users' queries share its CPU and memory, its token and its data credentials:
  the server can't tell users apart. For per-user isolation, run one server per user or group and point
  their sessions at it.
- **Results are collected in the engine.** By default Kyuubi's JDBC engine holds a statement's whole result
  in its heap (1 GiB, `kyuubi.engine.jdbc.memory`) before returning it. `kyuubi.operation.incremental.collect=true`
  streams instead; it hasn't been tried with this dialect.
- **The driver closes a statement after an error.** DuckDB's JDBC driver does that, which the dialect
  respects: one statement per step.
