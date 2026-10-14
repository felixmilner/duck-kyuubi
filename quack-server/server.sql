-- DuckDB Quack server for Kyuubi's duckdb engine (kyuubi-duckdb). QuackServer.java runs this file,
-- replacing each getenv call with its environment variable's value first: embedded DuckDB has none.
-- INSTALL is a no-op when the image already has the extension (quack-server/Dockerfile)
INSTALL quack;
INSTALL httpfs;
LOAD quack;
-- quack_serve needs DuckDB's secure crypto module, which httpfs brings
LOAD httpfs;

-- Your data. A demo table here; docs/duckdb.md shows how to attach an Iceberg REST catalog instead.
CREATE TABLE demo AS SELECT i AS id, 'row ' || i AS label, i % 3 = 0 AS fizz FROM range(1000) t(i);

-- Clients' SQL runs in this process. Without these two, anyone who can open a Kyuubi session could read
-- local files (/proc/self/environ holds this server's credentials) or change its settings.
SET disabled_filesystems = 'LocalFileSystem';
SET lock_configuration = true;

-- quack_serve returns the token as a result column; count(*) keeps it out of the log
SELECT count(*) AS quack_started FROM quack_serve('quack:0.0.0.0:9494', token := getenv('TOKEN'),
                                                  allow_other_hostname := true, disable_ssl := true);
