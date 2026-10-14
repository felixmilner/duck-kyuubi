package io.github.felixmilner.duckkyuubi.policy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kyuubi.plugin.SessionConfAdvisor;

/**
 * Kyuubi session policy (kyuubi.session.conf.advisor). It does two things:
 * - Sends each user's Spark engine to its own namespace, spark-<user>. That namespace holds only
 *   the user's own credentials (examples/kubernetes/add-user.sh), so code running in the engine can't reach
 *   anyone else's. A user without a namespace can't start Spark.
 * - Rejects a session that sets any config key not on the allowlist below. A denylist can't work:
 *   Kyuubi hands every non-"spark." key to spark-submit with "spark." prepended, so
 *   "kubernetes.namespace" would become spark.kubernetes.namespace. And kyuubi.engine.* keys can
 *   run code in the Kyuubi pod (e.g. kyuubi.engine.trino.java.options=-XX:OnError=...).
 * - Lets a session pick the JDBC engine's dialect: duckdb (the default, SQL on the Quack server) or
 *   launcher (jobs from the catalog as pods in spark-<user>; LauncherDialect in kyuubi-launcher). A
 *   launcher session gets the launcher connection provider and its own engine (subdomain "launcher"),
 *   so it never shares the user's DuckDB engine.
 * The overlay wins over anything the user sets (KyuubiSessionImpl applies it last).
 */
public class SessionPolicy implements SessionConfAdvisor {
  // DNS-1123 label once "spark-" is prepended; examples/kubernetes/add-user.sh checks the same
  private static final Pattern USER = Pattern.compile("[a-z]([a-z0-9-]{0,40}[a-z0-9])?");

  // Kyuubi's frontends add these to every session, and the Hive JDBC driver sends the last three
  private static final Set<String> RESERVED = Set.of(
      "kyuubi.client.ipAddress", "kyuubi.server.ipAddress", "kyuubi.session.connection.url",
      "kyuubi.session.real.user", "kyuubi.client.version", "use:database", "use:catalog");
  private static final Set<String> ALLOWED = Set.of(
      "kyuubi.engine.type", "kyuubi.session.name", "spark.driver.memory", "spark.driver.cores",
      "kyuubi.engine.jdbc.type");
  // everything under these runs inside the user's own engine pods
  private static final Set<String> ALLOWED_PREFIXES = Set.of("spark.sql.", "spark.executor.");
  private static final Set<String> ENGINE_TYPES = Set.of("JDBC", "SPARK_SQL");
  private static final Set<String> JDBC_TYPES = Set.of("duckdb", "launcher");

  @Override
  public Map<String, String> getConfOverlay(String user, Map<String, String> sessionConf) {
    if (!USER.matcher(user).matches()) {
      throw new IllegalArgumentException("user name " + user + " can't have a Spark namespace");
    }
    for (Map.Entry<String, String> entry : sessionConf.entrySet()) {
      String key = entry.getKey();
      if (!RESERVED.contains(key) && !ALLOWED.contains(key)
          && ALLOWED_PREFIXES.stream().noneMatch(key::startsWith)) {
        throw new IllegalArgumentException("session config " + key + " isn't allowed");
      }
      if (key.equals("kyuubi.engine.type") && !ENGINE_TYPES.contains(entry.getValue().toUpperCase())) {
        throw new IllegalArgumentException("engine type " + entry.getValue() + " isn't allowed");
      }
      if (key.equals("kyuubi.engine.jdbc.type") && !JDBC_TYPES.contains(entry.getValue())) {
        throw new IllegalArgumentException("JDBC engine type " + entry.getValue() + " isn't allowed");
      }
    }
    String namespace = "spark-" + user;
    Map<String, String> overlay = new HashMap<>(Map.of(
        "spark.kubernetes.namespace", namespace, "kyuubi.kubernetes.namespace", namespace));
    if ("launcher".equals(sessionConf.get("kyuubi.engine.jdbc.type"))) {
      overlay.put("kyuubi.engine.type", "JDBC");
      overlay.put("kyuubi.engine.jdbc.connection.provider", "launcher");
      overlay.put("kyuubi.engine.share.level.subdomain", "launcher");
      overlay.put("kyuubi.engine.jdbc.memory", "384m");   // it relays small results; the pod holds two engines
    }
    return overlay;
  }
}
