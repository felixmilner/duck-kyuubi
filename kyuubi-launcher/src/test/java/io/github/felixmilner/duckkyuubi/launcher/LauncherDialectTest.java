package io.github.felixmilner.duckkyuubi.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider;
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect;
import org.junit.jupiter.api.Test;

/**
 * Everything up to the Kubernetes API: statement parsing, parameter checks, the pod spec, answers as
 * result sets, the locked-down local DuckDB. The catalog is src/test/resources/catalog.json
 * (LAUNCHER_CATALOG, set by surefire).
 */
class LauncherDialectTest {

  private static JsonNode job() throws IOException {
    return Kube.JSON.readTree(Files.readString(Path.of(System.getenv("LAUNCHER_CATALOG")))).path("hello");
  }

  private static LauncherDialect.Params check(Map<String, String> given) throws Exception {
    return LauncherDialect.Params.check(job().path("params"), new java.util.LinkedHashMap<>(given));
  }

  // ------------------------------------------------------------------ parameters

  @Test
  void appliesDefaults() throws Exception {
    LauncherDialect.Params p = check(Map.of());
    assertEquals("1", p.cpu);
    assertEquals("512Mi", p.memory);
    assertEquals(List.of("--seconds", "5"), p.args);
    assertEquals("info", p.env.get("LOG_LEVEL"));
    assertEquals("384MB", p.env.get("DUCKDB_MEMORY_LIMIT"));   // 3/4 of the pod's memory
    assertEquals("1", p.env.get("DUCKDB_THREADS"));
    assertNull(p.nodeSelector);                                 // "any" maps to no selector
    assertEquals(600, p.timeoutSeconds(job()));                 // the job's timeout_s
  }

  @Test
  void mapsGivenParametersToArgsEnvResourcesNodeAndTimeout() throws Exception {
    LauncherDialect.Params p = check(Map.of("name", "world", "seconds", "30", "cpu", "2", "memory", "1Gi",
        "level", "debug", "node", "node-a", "timeout", "120"));
    assertEquals(List.of("--name", "world", "--seconds", "30"), p.args);
    assertEquals("2", p.cpu);
    assertEquals("1Gi", p.memory);
    assertEquals("debug", p.env.get("LOG_LEVEL"));
    assertEquals("768MB", p.env.get("DUCKDB_MEMORY_LIMIT"));
    assertEquals("2", p.env.get("DUCKDB_THREADS"));
    assertEquals("node-a", p.nodeSelector);
    assertEquals(120, p.timeoutSeconds(job()));
  }

  @Test
  void rejectsWhatTheCatalogDoesNotAllow() {
    String[][] bad = {
        {"image", "evil:latest", "unknown parameter"},
        {"name", "a;rm -rf /", "characters that aren't allowed"},
        {"name", "a b", "characters that aren't allowed"},
        {"seconds", "0", "must be between"},
        {"seconds", "ten", "must be a number"},
        {"cpu", "4", "must be between"},
        {"memory", "128Mi", "must be between"},
        {"memory", "4Gi", "must be between"},
        {"memory", "1G", "must look like"},
        {"level", "trace", "must be one of"},
        {"node", "node-b", "must be one of"},
    };
    for (String[] b : bad) {
      SQLException e = assertThrows(SQLException.class, () -> check(Map.of(b[0], b[1])), b[0] + "=" + b[1]);
      assertTrue(e.getMessage().contains(b[2]), b[0] + "=" + b[1] + ": " + e.getMessage());
    }
  }

  // ------------------------------------------------------------------ the pod

  @Test
  void buildsALockedDownPodInTheUsersNamespace() throws Exception {
    ObjectNode pod = LauncherDialect.podSpec("alice", "hello", "hello-1a2b3c4d", job(),
        check(Map.of("name", "world", "cpu", "2", "memory", "1Gi", "node", "node-a")));
    assertEquals("hello-1a2b3c4d", pod.at("/metadata/name").asText());
    assertEquals("kyuubi-launcher", pod.at("/metadata/labels/app.kubernetes.io~1managed-by").asText());
    assertEquals("hello", pod.at("/metadata/labels/duck-kyuubi~1job").asText());
    assertEquals("alice", pod.at("/metadata/labels/duck-kyuubi~1user").asText());

    JsonNode spec = pod.path("spec");
    assertEquals("Never", spec.path("restartPolicy").asText());
    assertFalse(spec.path("automountServiceAccountToken").asBoolean(true));
    assertEquals(600, spec.path("activeDeadlineSeconds").asLong());
    assertEquals("node-a", spec.at("/nodeSelector/kubernetes.io~1hostname").asText());

    JsonNode c = spec.path("containers").path(0);
    assertEquals("python:3.12-slim", c.path("image").asText());
    assertEquals("Always", c.path("imagePullPolicy").asText());
    assertEquals(List.of("python", "-u", "-c", "print('hi')"), texts(c.path("command")));
    assertEquals(List.of("--greeting", "hello", "--name", "world", "--seconds", "5"), texts(c.path("args")));
    assertEquals("hello-secret", c.at("/envFrom/0/secretRef/name").asText());
    Map<String, String> env = new java.util.HashMap<>();
    c.path("env").forEach(e -> env.put(e.path("name").asText(), e.path("value").asText()));
    assertEquals("alice", env.get("KYUUBI_USER"));
    assertEquals("hello-1a2b3c4d", env.get("LAUNCHER_RUN"));
    assertEquals("info", env.get("LOG_LEVEL"));
    assertEquals("2", c.at("/resources/limits/cpu").asText());
    assertEquals("500m", c.at("/resources/requests/cpu").asText());
    assertEquals("1Gi", c.at("/resources/limits/memory").asText());
    assertEquals("1Gi", c.at("/resources/requests/memory").asText());
    assertFalse(c.at("/securityContext/allowPrivilegeEscalation").asBoolean(true));
    assertTrue(c.at("/securityContext/runAsNonRoot").asBoolean(false));
    assertEquals("ALL", c.at("/securityContext/capabilities/drop/0").asText());
  }

  @Test
  void theCatalogCanSetThePullPolicy() throws Exception {
    ObjectNode job = ((ObjectNode) job()).deepCopy().put("image_pull_policy", "IfNotPresent");
    ObjectNode pod = LauncherDialect.podSpec("alice", "hello", "hello-1", job, check(Map.of()));
    assertEquals("IfNotPresent", pod.at("/spec/containers/0/imagePullPolicy").asText());
  }

  private static List<String> texts(JsonNode array) {
    List<String> out = new ArrayList<>();
    array.forEach(n -> out.add(n.asText()));
    return out;
  }

  // ------------------------------------------------------------------ answers

  @Test
  void answersAreRealResultSets() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
         Statement statement = connection.createStatement()) {
      String sql = LauncherDialect.values(List.of("run", "n", "line"),
          List.of(List.of("hello-1", 1, "it's"), List.of("hello-1", 2, "{\"ok\": true}")));
      try (ResultSet rs = statement.executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals("hello-1", rs.getString("run"));
        assertEquals(1, rs.getInt("n"));
        assertEquals("it's", rs.getString("line"));
        assertTrue(rs.next());
        assertEquals("{\"ok\": true}", rs.getString("line"));
        assertFalse(rs.next());
      }
      try (ResultSet rs = statement.executeQuery(LauncherDialect.values(List.of("run", "job"), List.of()))) {
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void runsOnlyLauncherCommands() throws Exception {
    LauncherDialect dialect = new LauncherDialect();   // no Kubernetes needed until a command calls it
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
      LauncherConnectionProvider.USERS.put(connection, "alice");
      try (Statement statement = dialect.createStatement(connection, 1000)) {
        try (ResultSet rs = statement.executeQuery("show jobs;")) {
          assertTrue(rs.next());
          assertEquals("hello", rs.getString("job"));
          assertEquals("python:3.12-slim", rs.getString("image"));
          assertTrue(rs.getString("params").contains("\"seconds\""));
          assertFalse(rs.next());
        }
        try (ResultSet rs = statement.executeQuery("SELECT 1")) {   // the JDBC engine's start-up check
          assertTrue(rs.next());
        }
      }
      String[][] refused = {
          {"SELECT * FROM read_csv('/etc/passwd')", "launcher sessions accept only"},
          {"RUN hello; DROP TABLE x", "launcher sessions accept only"},
          {"RUN nope", "no job nope in the catalog"},
          {"RUN hello image=evil", "unknown parameter image"},
          {"RUN hello seconds=9999", "must be between"},
      };
      for (String[] r : refused) {
        // DuckDB's driver closes a statement that failed, so one statement per attempt
        try (Statement statement = dialect.createStatement(connection, 1000)) {
          SQLException e = assertThrows(SQLException.class, () -> statement.execute(r[0]), r[0]);
          assertTrue(e.getMessage().contains(r[1]), r[0] + ": " + e.getMessage());
        }
      }
    }
  }

  // ------------------------------------------------------------------ the connection

  @Test
  void theLocalDuckDBIsLockedDown() throws Exception {
    Path secret = Files.createTempFile("launcher", ".csv");
    Files.writeString(secret, "a\n1\n");
    KyuubiConf conf = new KyuubiConf(false)
        .set("kyuubi.engine.jdbc.connection.url", "jdbc:duckdb:")
        .set("kyuubi.session.user", "alice");
    try (Connection connection = new LauncherConnectionProvider().getConnection(conf)) {
      assertEquals("alice", LauncherConnectionProvider.USERS.get(connection));
      for (String sql : new String[] {
          "SELECT * FROM read_csv('" + secret + "')",
          "SET enable_external_access = true",
          "INSTALL httpfs"}) {
        try (Statement statement = connection.createStatement()) {
          assertThrows(SQLException.class, () -> statement.execute(sql), sql);
        }
      }
    } finally {
      Files.delete(secret);
    }
  }

  @Test
  void needsToKnowItsUser() {
    KyuubiConf conf = new KyuubiConf(false).set("kyuubi.engine.jdbc.connection.url", "jdbc:duckdb:");
    assertThrows(IllegalStateException.class, () -> new LauncherConnectionProvider().getConnection(conf));
  }

  @Test
  void isRegisteredForKyuubisServiceLoader() {
    // Kyuubi instantiates every registered dialect to find one by name, so this must work outside a pod
    List<JdbcDialect> dialects = new ArrayList<>();
    ServiceLoader.load(JdbcDialect.class).forEach(dialects::add);
    assertTrue(dialects.stream().anyMatch(d -> d instanceof LauncherDialect && d.name().equals("launcher")));
    List<JdbcConnectionProvider> providers = new ArrayList<>();
    ServiceLoader.load(JdbcConnectionProvider.class).forEach(providers::add);
    assertTrue(providers.stream().anyMatch(p -> p instanceof LauncherConnectionProvider && p.name().equals("launcher")));
  }
}
