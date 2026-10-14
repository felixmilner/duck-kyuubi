package io.github.felixmilner.duckkyuubi.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider;
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect;
import org.junit.jupiter.api.Test;

/** What can be checked without a Quack server: statement wrapping, value conversion, registration. */
class DuckDBDialectTest {

  @Test
  void shipsTheWholeStatementToTheServer() {
    assertEquals("SELECT * FROM quack_query_by_name('remote', 'SELECT 1')", DuckDBDialect.remote("SELECT 1"));
  }

  @Test
  void stripsTrailingSemicolonsAndDoublesQuotes() {
    assertEquals("SELECT * FROM quack_query_by_name('remote', 'SELECT ''it''''s'' AS x')",
        DuckDBDialect.remote("  SELECT 'it''s' AS x ; ;\n"));
  }

  @Test
  void convertsTinyintAndBlobValuesForKyuubisRowGenerator() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
         Statement statement = connection.createStatement();
         ResultSet raw = statement.executeQuery(
             "SELECT 7::TINYINT AS t, '\\x00\\xFFhi\\x5C'::BLOB AS b, 'text' AS s")) {
      ResultSet rs = DuckDBDialect.readable(raw);
      assertTrue(rs.next());
      assertEquals(Short.valueOf((short) 7), rs.getObject(1));
      // DuckDB's text form: printable ASCII as is, everything else (backslash and quotes too) as \xNN
      assertEquals("\\x00\\xFFhi\\x5C", rs.getObject(2));
      assertEquals("text", rs.getObject(3));
    }
  }

  @Test
  void isRegisteredForKyuubisServiceLoader() {
    // looked up by name, as Kyuubi does; the engine jar registers its built-in dialects too
    JdbcDialect dialect = ServiceLoader.load(JdbcDialect.class).stream().map(ServiceLoader.Provider::get)
        .filter(d -> d.name().equals("duckdb")).collect(Collectors.toList()).get(0);
    assertTrue(dialect instanceof DuckDBDialect);
    JdbcConnectionProvider provider = ServiceLoader.load(JdbcConnectionProvider.class).stream()
        .map(ServiceLoader.Provider::get).filter(p -> p.name().equals("duckdb")).findFirst().orElseThrow();
    assertTrue(provider instanceof DuckDBConnectionProvider);
    assertEquals("org.duckdb.DuckDBDriver", provider.driverClass());
  }

  @Test
  void refusesToConnectWithoutAToken() {
    // QUACK_TOKEN isn't set in the test environment
    KyuubiConf conf = new KyuubiConf(false).set("kyuubi.engine.jdbc.connection.url", "jdbc:duckdb:");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> new DuckDBConnectionProvider().getConnection(conf));
    assertTrue(e.getMessage().contains("QUACK_TOKEN"));
  }
}
