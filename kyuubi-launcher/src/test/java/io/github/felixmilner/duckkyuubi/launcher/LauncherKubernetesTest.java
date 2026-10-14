package io.github.felixmilner.duckkyuubi.launcher;

import static io.github.felixmilner.duckkyuubi.launcher.FakeKubernetes.terminated;
import static io.github.felixmilner.duckkyuubi.launcher.FakeKubernetes.waiting;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The launcher's statements against a fake Kubernetes API (FakeKubernetes), as Kyuubi user alice:
 * what it asks the API server for, and what it answers.
 */
class LauncherKubernetesTest {
  private static final String ALICE = "/api/v1/namespaces/spark-alice/pods";

  private FakeKubernetes k8s;
  private LauncherDialect dialect;
  private Connection connection;

  @BeforeEach
  void setUp() throws Exception {
    k8s = new FakeKubernetes();
    dialect = new LauncherDialect(k8s.kube(), 10);
    connection = DriverManager.getConnection("jdbc:duckdb:");
    LauncherConnectionProvider.USERS.put(connection, "alice");
  }

  @AfterEach
  void tearDown() throws Exception {
    connection.close();
    k8s.close();
  }

  private List<Map<String, Object>> rows(String sql) throws SQLException {
    try (Statement statement = dialect.createStatement(connection, 1000);
         ResultSet rs = statement.executeQuery(sql)) {
      ResultSetMetaData meta = rs.getMetaData();
      List<Map<String, Object>> rows = new ArrayList<>();
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          row.put(meta.getColumnName(i), rs.getObject(i));
        }
        rows.add(row);
      }
      return rows;
    }
  }

  private Map<String, Object> row(String sql) throws SQLException {
    List<Map<String, Object>> rows = rows(sql);
    assertEquals(1, rows.size(), sql);
    return rows.get(0);
  }

  private String error(String sql) throws SQLException {
    try (Statement statement = dialect.createStatement(connection, 1000)) {
      return assertThrows(SQLException.class, () -> statement.execute(sql), sql).getMessage();
    }
  }

  private ObjectNode onlyPod() {
    assertEquals(1, k8s.pods.size(), k8s.pods.keySet().toString());
    return k8s.pods.values().iterator().next();
  }

  // ------------------------------------------------------------------ RUN

  @Test
  void runAsyncSubmitsThePodInTheUsersNamespace() throws Exception {
    Map<String, Object> row = row("RUN hello async=true name=world");
    assertEquals("Submitted", row.get("status"));
    assertEquals("hello", row.get("job"));
    String run = (String) row.get("run");
    assertTrue(run.matches("hello-[0-9a-f]{8}"), run);

    assertEquals(1, k8s.count("POST " + ALICE));
    JsonNode pod = k8s.pods.get("spark-alice/" + run);
    assertEquals("alice", pod.at("/metadata/labels/duck-kyuubi~1user").asText());
    assertTrue(pod.at("/spec/containers/0/args").toString().contains("\"--name\",\"world\""));
    assertTrue(k8s.authorizations.stream().allMatch("Bearer test-token"::equals));
  }

  @Test
  void runWaitsForThePodAndReturnsItsSummary() throws Exception {
    k8s.onCreate = pod -> {
      terminated(pod, "Succeeded", 0);
      k8s.log("spark-alice", pod, "hello\nworking\n{\"ok\": true}\n");
    };
    Map<String, Object> row = row("RUN hello");
    assertEquals("Succeeded", row.get("status"));
    assertEquals(0, ((Number) row.get("exit_code")).intValue());
    assertEquals("{\"ok\": true}", row.get("summary"));
    assertTrue(((String) row.get("log_tail")).contains("working"));
  }

  @Test
  void runPollsUntilThePodEnds() throws Exception {
    AtomicInteger gets = new AtomicInteger();
    k8s.onGet = pod -> {
      int n = gets.incrementAndGet();
      if (n == 2) {
        pod.putObject("status").put("phase", "Running");
        k8s.log("spark-alice", pod, "tick\n");
      } else if (n == 4) {
        terminated(pod, "Succeeded", 0);
        k8s.log("spark-alice", pod, "tick\ntock\n{\"done\": 1}\n");
      }
    };
    Map<String, Object> row = row("RUN hello");
    assertEquals("Succeeded", row.get("status"));
    assertEquals("{\"done\": 1}", row.get("summary"));
    assertTrue(gets.get() >= 4, "polled " + gets.get() + " times");
  }

  @Test
  void aFailedRunIsAnErrorWithTheCatalogsMeaningAndTheLog() throws Exception {
    k8s.onCreate = pod -> {
      terminated(pod, "Failed", 3);
      k8s.log("spark-alice", pod, "step 1\nboom\n");
    };
    String message = error("RUN hello");
    assertTrue(message.contains("ended Failed with exit code 3 (asked to fail)"), message);
    assertTrue(message.contains("boom"), message);
  }

  @Test
  void aPodThatCantPullItsImageIsDeleted() throws Exception {
    k8s.onCreate = pod -> waiting(pod, "ImagePullBackOff");
    String message = error("RUN hello");
    assertTrue(message.contains("can't start: ImagePullBackOff"), message);
    assertTrue(k8s.pods.isEmpty());
    assertEquals(1, k8s.count("DELETE " + ALICE + "/hello-"));
  }

  @Test
  void cancellingAWaitingRunDeletesItsPod() throws Exception {
    k8s.onCreate = pod -> {
      pod.putObject("status").put("phase", "Running");
      k8s.log("spark-alice", pod, "tick\n");
    };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Statement statement = dialect.createStatement(connection, 1000)) {
      Future<Boolean> waiting = executor.submit(() -> statement.execute("RUN hello"));
      // the RUN is waiting once it has polled its pod
      long deadline = System.currentTimeMillis() + 10_000;
      while (k8s.count("GET " + ALICE + "/hello-") == 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      String run = onlyPod().at("/metadata/name").asText();
      statement.cancel();
      ExecutionException e = assertThrows(ExecutionException.class, () -> waiting.get(10, TimeUnit.SECONDS));
      assertInstanceOf(SQLException.class, e.getCause());
      assertTrue(e.getCause().getMessage().contains("run " + run + " was cancelled"), e.getCause().getMessage());
      assertTrue(k8s.pods.isEmpty());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void runRemovesFinishedRunsOlderThanADay() throws Exception {
    terminated(k8s.put("spark-alice", "hello-0000old1", "hello", "Succeeded", Duration.ofDays(2)), "Succeeded", 0);
    k8s.put("spark-alice", "hello-0000old2", "hello", "Running", Duration.ofDays(2));
    terminated(k8s.put("spark-alice", "hello-0000new1", "hello", "Failed", Duration.ofHours(1)), "Failed", 1);
    k8s.put("spark-alice", "unmanaged-pod", null, "Succeeded", Duration.ofDays(2));
    row("RUN hello async=true");
    assertFalse(k8s.pods.containsKey("spark-alice/hello-0000old1"));
    assertTrue(k8s.pods.containsKey("spark-alice/hello-0000old2"));   // still running
    assertTrue(k8s.pods.containsKey("spark-alice/hello-0000new1"));   // kept a day for STATUS and LOGS
    assertTrue(k8s.pods.containsKey("spark-alice/unmanaged-pod"));    // not the launcher's
    assertEquals(4, k8s.pods.size());
  }

  // ------------------------------------------------------------------ STATUS, LOGS, SHOW RUNS, CANCEL

  @Test
  void statusReportsPhaseExitCodeSummaryAndMeaning() throws Exception {
    ObjectNode failed = terminated(k8s.put("spark-alice", "hello-00000001", "hello", "Failed", Duration.ofMinutes(5)),
        "Failed", 3);
    k8s.log("spark-alice", failed, "x\n{\"rows\": 5}\nbye\n");
    Map<String, Object> row = row("STATUS hello-00000001");
    assertEquals("hello", row.get("job"));
    assertEquals("Failed", row.get("phase"));
    assertEquals(3, ((Number) row.get("exit_code")).intValue());
    assertEquals("{\"rows\": 5}", row.get("summary"));
    assertEquals("asked to fail", row.get("message"));

    waiting(k8s.put("spark-alice", "hello-00000002", "hello", "Pending", Duration.ZERO), "ContainerCreating");
    row = row("STATUS hello-00000002");
    assertEquals("Pending", row.get("phase"));
    assertEquals(-1, ((Number) row.get("exit_code")).intValue());
    assertEquals("ContainerCreating", row.get("message"));

    ObjectNode going = k8s.put("spark-alice", "hello-00000003", "hello", "Running", Duration.ZERO);
    ((ObjectNode) going.path("metadata")).put("deletionTimestamp", "2026-01-01T00:00:00Z");
    assertEquals("Terminating", row("STATUS hello-00000003").get("phase"));
  }

  @Test
  void logsPageFromALine() throws Exception {
    ObjectNode pod = k8s.put("spark-alice", "hello-00000001", "hello", "Running", Duration.ZERO);
    k8s.log("spark-alice", pod, "l1\nl2\nl3\nl4\nl5\n");
    List<Map<String, Object>> rows = rows("LOGS hello-00000001 from=3");
    assertEquals(List.of(3, 4, 5), rows.stream().map(r -> ((Number) r.get("n")).intValue()).toList());
    assertEquals(List.of("l3", "l4", "l5"), rows.stream().map(r -> r.get("line")).toList());
    assertEquals(5, rows("LOGS hello-00000001").size());
    assertTrue(error("LOGS hello-00000001 from=x").contains("from must be a line number"));

    waiting(k8s.put("spark-alice", "hello-00000002", "hello", "Pending", Duration.ZERO), "ContainerCreating");
    assertEquals(0, rows("LOGS hello-00000002").size());   // not started yet: no log, no error
  }

  @Test
  void showRunsListsOnlyTheUsersLauncherRuns() throws Exception {
    k8s.put("spark-alice", "hello-00000001", "hello", "Running", Duration.ZERO);
    k8s.put("spark-alice", "unmanaged-pod", null, "Running", Duration.ZERO);
    k8s.put("spark-bob", "hello-00000002", "hello", "Running", Duration.ZERO);
    List<Map<String, Object>> rows = rows("SHOW RUNS");
    assertEquals(1, rows.size());
    assertEquals("hello-00000001", rows.get(0).get("run"));
    assertEquals("hello", rows.get(0).get("job"));
    assertEquals("Running", rows.get(0).get("phase"));
    assertEquals(1, k8s.count("GET " + ALICE + "?labelSelector=app.kubernetes.io%2Fmanaged-by%3Dkyuubi-launcher"));
  }

  @Test
  void cancelDeletesTheRunsPod() throws Exception {
    k8s.put("spark-alice", "hello-00000001", "hello", "Running", Duration.ZERO);
    Map<String, Object> row = row("CANCEL hello-00000001");
    assertEquals("cancelled", row.get("status"));
    assertTrue(k8s.pods.isEmpty());
  }

  @Test
  void onlyLauncherRunsCanBeReadOrCancelled() throws Exception {
    ObjectNode pod = k8s.put("spark-alice", "other-pod", null, "Running", Duration.ZERO);
    k8s.log("spark-alice", pod, "private\n");
    for (String sql : new String[] {"STATUS other-pod", "LOGS other-pod", "CANCEL other-pod"}) {
      String message = error(sql);
      assertTrue(message.contains("other-pod is not a launcher run"), sql + ": " + message);
    }
    assertTrue(k8s.pods.containsKey("spark-alice/other-pod"));
    assertEquals(0, k8s.count("DELETE"));
  }

  @Test
  void usersReachOnlyTheirOwnNamespace() throws Exception {
    k8s.put("spark-bob", "hello-0000000b", "hello", "Running", Duration.ZERO);
    for (String sql : new String[] {"STATUS hello-0000000b", "LOGS hello-0000000b", "CANCEL hello-0000000b"}) {
      String message = error(sql);
      assertTrue(message.contains("HTTP 404"), sql + ": " + message);
    }
    assertTrue(k8s.pods.containsKey("spark-bob/hello-0000000b"));
    assertTrue(k8s.requests.stream().allMatch(r -> r.contains(" " + ALICE)), k8s.requests.toString());
  }

  // ------------------------------------------------------------------ the API server's errors

  @Test
  void kubernetesErrorsAreReported() throws Exception {
    k8s.fail("POST", 403, "pods is forbidden: User \"system:serviceaccount:duck-kyuubi:kyuubi\" cannot create"
        + " resource \"pods\" in the namespace \"spark-alice\"");
    String message = error("RUN hello async=true");
    assertTrue(message.startsWith("launcher: Kubernetes API POST " + ALICE + ": HTTP 403: pods is forbidden"), message);
  }
}
