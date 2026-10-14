import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs a DuckDB Quack server on the pinned DuckDB JDBC build, so the server and Kyuubi's JDBC engine
 * use the same DuckDB version and its published extensions. Usage: java -cp duckdb_jdbc.jar
 * QuackServer.java server.sql
 *
 * <p>Embedded DuckDB has no getenv(), so getenv('NAME') in the SQL file is replaced with the value
 * of the environment variable as a string literal, wherever it appears (in comments too: a variable
 * that isn't set stops the server). Those values are redacted from any error message.
 */
public class QuackServer {
  private static final Pattern GETENV = Pattern.compile("getenv\\('([A-Za-z_][A-Za-z0-9_]*)'\\)");

  public static void main(String[] args) throws Exception {
    String sql = Files.readString(Path.of(args[0]));
    List<String> secrets = new ArrayList<>();
    Matcher m = GETENV.matcher(sql);
    StringBuilder resolved = new StringBuilder();
    while (m.find()) {
      String value = System.getenv(m.group(1));
      if (value == null || value.isEmpty()) {
        throw new IllegalStateException("environment variable " + m.group(1) + " is not set");
      }
      secrets.add(value);
      m.appendReplacement(resolved, Matcher.quoteReplacement("'" + value.replace("'", "''") + "'"));
    }
    m.appendTail(resolved);

    Connection connection = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement statement = connection.createStatement()) {
      statement.execute(resolved.toString());
    } catch (Exception e) {
      String message = String.valueOf(e.getMessage());
      for (String secret : secrets) {
        message = message.replace(secret, "***");
      }
      System.err.println("server.sql failed: " + message);
      System.exit(1);
    }
    System.out.println("Quack server started");
    // quack_serve runs in the background for as long as the connection stays open
    Thread.currentThread().join();
  }
}
