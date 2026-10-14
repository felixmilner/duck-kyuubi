package io.github.felixmilner.duckkyuubi.duckdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect;
import org.apache.kyuubi.engine.jdbc.schema.DefaultJdbcTRowSetGenerator;
import org.apache.kyuubi.engine.jdbc.schema.JdbcTRowSetGenerator;
import org.apache.kyuubi.engine.jdbc.schema.SchemaHelper;

/**
 * Kyuubi JDBC-engine dialect ("kyuubi.engine.jdbc.type=duckdb") that runs every statement on a
 * shared DuckDB Quack server (QUACK_URI). The engine's own DuckDB is only a client:
 * DuckDBConnectionProvider attaches the server as "remote", and each statement becomes
 * SELECT * FROM quack_query_by_name('remote', '<statement>')
 * All statements of a Kyuubi session go over that one attachment, so they share a server-side
 * session: temp tables, USE, SET VARIABLE and transactions carry over between statements.
 * Remote Iceberg tables don't bind as remote.<schema>.<table> on DuckDB v2.0.0-alpha, which is why
 * the whole statement is shipped (docs/duckdb.md).
 */
public class DuckDBDialect extends JdbcDialect {
  static final String QUACK_URI =
      System.getenv().getOrDefault("QUACK_URI", "quack:localhost:9494");
  static final String REMOTE = "remote";

  private static final Set<String> SQL_METHODS = Set.of(
      "execute", "executeQuery", "executeUpdate", "executeLargeUpdate", "addBatch");

  @Override
  public String name() {
    return "duckdb";
  }

  @Override
  public Statement createStatement(Connection connection, int fetchSize) {
    Statement statement;
    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return (Statement) Proxy.newProxyInstance(
        getClass().getClassLoader(),
        new Class<?>[] {Statement.class},
        (proxy, method, args) -> {
          if (SQL_METHODS.contains(method.getName()) && args != null && args[0] instanceof String) {
            args[0] = remote((String) args[0]);
          }
          Object result = invoke(method, statement, args);
          return result instanceof ResultSet ? readable((ResultSet) result) : result;
        });
  }

  static String remote(String sql) {
    String body = sql.strip();
    while (body.endsWith(";")) {
      body = body.substring(0, body.length() - 1).strip();
    }
    return "SELECT * FROM quack_query_by_name('" + REMOTE + "', '" + body.replace("'", "''") + "')";
  }

  /**
   * Kyuubi's generic row generator reads TINYINT columns as Short (DuckDB returns Byte) and maps
   * BLOB to a string column via toString() (DuckDBBlobResult has no useful one), so convert both
   * as they're read.
   */
  static ResultSet readable(ResultSet resultSet) {
    return (ResultSet) Proxy.newProxyInstance(
        DuckDBDialect.class.getClassLoader(),
        new Class<?>[] {ResultSet.class},
        (proxy, method, args) -> {
          Object value = invoke(method, resultSet, args);
          if (!method.getName().equals("getObject") || args.length != 1) {
            return value;
          }
          if (value instanceof Byte) {
            return ((Byte) value).shortValue();
          }
          if (value instanceof Blob) {
            return blobText((Blob) value);
          }
          return value;
        });
  }

  /** DuckDB's own text form for BLOBs: printable ASCII as is, other bytes as \xNN. */
  private static String blobText(Blob blob) throws SQLException {
    byte[] bytes = blob.getBytes(1, (int) blob.length());
    StringBuilder text = new StringBuilder(bytes.length * 4);
    for (byte b : bytes) {
      if (b >= 32 && b <= 126 && b != '\\' && b != '\'' && b != '"') {
        text.append((char) b);
      } else {
        text.append(String.format("\\x%02X", b));
      }
    }
    return text.toString();
  }

  private static Object invoke(Method method, Object target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Override
  public JdbcTRowSetGenerator getTRowSetGenerator() {
    return new DefaultJdbcTRowSetGenerator();
  }

  @Override
  public SchemaHelper getSchemaHelper() {
    return new SchemaHelper() {};
  }
}
