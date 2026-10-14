package io.github.felixmilner.duckkyuubi.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

class SessionPolicyTest {
  private final SessionPolicy policy = new SessionPolicy();

  @Test
  void sendsEveryUserToTheirOwnNamespace() {
    Map<String, String> overlay = policy.getConfOverlay("alice", Map.of());
    assertEquals("spark-alice", overlay.get("spark.kubernetes.namespace"));
    assertEquals("spark-alice", overlay.get("kyuubi.kubernetes.namespace"));
    assertFalse(overlay.containsKey("kyuubi.engine.jdbc.connection.provider"));
  }

  @Test
  void allowsTheAllowlistAndWhatKyuubisFrontendsAdd() {
    policy.getConfOverlay("alice", Map.of(
        "kyuubi.engine.type", "jdbc", "kyuubi.engine.jdbc.type", "duckdb", "kyuubi.session.name", "x",
        "spark.sql.shuffle.partitions", "8", "spark.executor.memory", "2g", "spark.driver.memory", "1g",
        "kyuubi.client.ipAddress", "10.0.0.1", "use:database", "default"));
  }

  @Test
  void rejectsAnythingElse() {
    for (Map<String, String> conf : new Map[] {
        Map.of("kyuubi.engine.jdbc.connection.url", "jdbc:postgresql://elsewhere/db"),
        Map.of("kyuubi.engine.jdbc.java.options", "-XX:OnError=sh"),
        Map.of("kubernetes.namespace", "kube-system"),
        Map.of("spark.kubernetes.namespace", "kube-system"),
        Map.of("kyuubi.engine.share.level.subdomain", "launcher")}) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> policy.getConfOverlay("alice", conf));
      assertTrue(e.getMessage().contains("isn't allowed"), e.getMessage());
    }
  }

  @Test
  void rejectsOtherEnginesAndDialects() {
    assertThrows(IllegalArgumentException.class,
        () -> policy.getConfOverlay("alice", Map.of("kyuubi.engine.type", "TRINO")));
    assertThrows(IllegalArgumentException.class,
        () -> policy.getConfOverlay("alice", Map.of("kyuubi.engine.jdbc.type", "postgresql")));
  }

  @Test
  void givesLauncherSessionsTheirOwnEngine() {
    Map<String, String> overlay = policy.getConfOverlay("alice", Map.of("kyuubi.engine.jdbc.type", "launcher"));
    assertEquals("JDBC", overlay.get("kyuubi.engine.type"));
    assertEquals("launcher", overlay.get("kyuubi.engine.jdbc.connection.provider"));
    assertEquals("launcher", overlay.get("kyuubi.engine.share.level.subdomain"));
    assertEquals("384m", overlay.get("kyuubi.engine.jdbc.memory"));
  }

  @Test
  void rejectsUserNamesThatCantBeANamespace() {
    for (String user : new String[] {"Alice", "al_ice", "-alice", "alice-", "a".repeat(43)}) {
      assertThrows(IllegalArgumentException.class, () -> policy.getConfOverlay(user, Map.of()), user);
    }
  }
}
