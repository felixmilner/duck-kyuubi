package io.github.felixmilner.duckkyuubi.launcher;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider;

/**
 * Connection provider for launcher sessions ("kyuubi.engine.jdbc.type=launcher"; see LauncherDialect).
 * The connection is an in-memory DuckDB that only formats the launcher's answers as result sets. User
 * SQL never runs on it (the dialect accepts launcher commands only), and it is locked down anyway,
 * because it runs inside the Kyuubi pod, next to the server's own credentials (QUACK_TOKEN and the like).
 */
public class LauncherConnectionProvider extends JdbcConnectionProvider {
  /** The Kyuubi user each connection serves; the dialect launches into spark-<user>. */
  static final Map<Connection, String> USERS = Collections.synchronizedMap(new WeakHashMap<>());

  @Override
  public String name() {
    return "launcher";
  }

  @Override
  public String driverClass() {
    return "org.duckdb.DuckDBDriver";
  }

  @Override
  public Connection getConnection(KyuubiConf conf) {
    scala.Option<String> user = conf.getOption("kyuubi.session.user");
    if (user.isEmpty() || user.get().isEmpty()) {
      throw new IllegalStateException("launcher: the engine doesn't know its Kyuubi user (kyuubi.session.user)");
    }
    Connection connection = super.getConnection(conf);
    try (Statement statement = connection.createStatement()) {
      statement.execute("SET enable_external_access = false");
      statement.execute("SET autoinstall_known_extensions = false");
      statement.execute("SET autoload_known_extensions = false");
      statement.execute("SET lock_configuration = true");
    } catch (SQLException e) {
      try {
        connection.close();
      } catch (SQLException ignored) {
        // closing a connection we're discarding
      }
      throw new IllegalStateException("launcher: locking down the local DuckDB failed", e);
    }
    USERS.put(connection, user.get());
    return connection;
  }
}
