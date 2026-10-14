package io.github.felixmilner.duckkyuubi.duckdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider;

/**
 * Opens the engine's embedded DuckDB, gives it a quack secret and attaches the Quack server as
 * "remote" for DuckDBDialect. The secret means the token never appears in a statement Kyuubi sees
 * or logs. The token comes from the engine's environment (QUACK_TOKEN, inherited from the Kyuubi
 * pod); embedded DuckDB has no getenv() and CREATE SECRET takes no bind parameters, so it is
 * inlined here.
 */
public class DuckDBConnectionProvider extends JdbcConnectionProvider {
  @Override
  public String name() {
    return "duckdb";
  }

  @Override
  public String driverClass() {
    return "org.duckdb.DuckDBDriver";
  }

  @Override
  public Connection getConnection(KyuubiConf conf) {
    String token = System.getenv("QUACK_TOKEN");
    if (token == null || token.isEmpty()) {
      throw new IllegalStateException("QUACK_TOKEN is not set in the engine environment");
    }
    Connection connection = super.getConnection(conf);
    // DuckDB's JDBC driver closes a Statement when it fails, so one Statement per step
    try {
      try (Statement statement = connection.createStatement()) {
        statement.execute("INSTALL quack");
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute("LOAD quack");
      }
    } catch (SQLException e) {
      closeQuietly(connection);
      throw new IllegalStateException("DuckDB: loading the quack extension failed", e);
    }
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE SECRET quack (TYPE quack, TOKEN '" + quote(token) + "')");
    } catch (SQLException e) {
      closeQuietly(connection);
      // not chained: the error message could echo the statement, and with it the token
      throw new IllegalStateException("DuckDB: creating the quack secret failed");
    }
    // an Iceberg catalog on the server must be attached with TABLE_RESOLUTION 'eager' (docs/duckdb.md)
    try (Statement statement = connection.createStatement()) {
      statement.execute("ATTACH '" + quote(DuckDBDialect.QUACK_URI) + "' AS "
          + DuckDBDialect.REMOTE + " (DISABLE_SSL true)");
    } catch (SQLException e) {
      closeQuietly(connection);
      throw new IllegalStateException("DuckDB: attaching the Quack server failed", e);
    }
    return connection;
  }

  private static String quote(String literal) {
    return literal.replace("'", "''");
  }

  private static void closeQuietly(Connection connection) {
    try {
      connection.close();
    } catch (SQLException ignored) {
      // already failing
    }
  }
}
