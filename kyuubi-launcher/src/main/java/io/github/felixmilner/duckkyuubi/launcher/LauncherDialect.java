package io.github.felixmilner.duckkyuubi.launcher;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect;
import org.apache.kyuubi.engine.jdbc.schema.DefaultJdbcTRowSetGenerator;
import org.apache.kyuubi.engine.jdbc.schema.JdbcTRowSetGenerator;
import org.apache.kyuubi.engine.jdbc.schema.SchemaHelper;

/**
 * Kyuubi JDBC-engine dialect ("kyuubi.engine.jdbc.type=launcher") that runs jobs from a catalog as pods,
 * so Kyuubi is the control plane for batch jobs as well as queries: same login, sessions, REST/Thrift API,
 * per-user namespace and credentials, cancel. Statements:
 *
 *   SHOW JOBS                      the catalog: job names, images, parameters
 *   RUN <job> [name=value ...]     start the job as a pod in spark-<user>, wait for it, return its result.
 *                                  While it waits, the pod's log goes to the engine's log (the JDBC engine
 *                                  keeps no operation log a client could read). With async=true it returns
 *                                  the run id at once; clients follow a run with STATUS and LOGS.
 *   STATUS <run>                   phase, exit code, seconds and the run summary (once finished)
 *   SHOW RUNS                      this user's job pods
 *   LOGS <run> [from=N]            a run's log as (n, line) rows from line N (default 1), 2000 lines at most
 *   CANCEL <run>                   delete a run's pod (cancelling a waiting RUN statement does the same)
 *
 * The catalog ($LAUNCHER_CATALOG, default /opt/kyuubi/conf/jobs/catalog.json) fixes each job's image,
 * command, Secrets and security context; users only pass the parameters it declares, within its limits. The
 * pod runs in the user's namespace with the Secrets the job names from there (e.g. the user's own catalog
 * credential), without a service-account token, as non-root with no capabilities. Nothing else runs: any other
 * statement is refused, and the local DuckDB that formats the answers is locked down
 * (LauncherConnectionProvider).
 */
public class LauncherDialect extends JdbcDialect {
  private static final Path CATALOG =
      Path.of(System.getenv().getOrDefault("LAUNCHER_CATALOG", "/opt/kyuubi/conf/jobs/catalog.json"));
  private static final Set<String> SQL_METHODS = Set.of("execute", "executeQuery");
  private static final Pattern RUN = Pattern.compile("(?is)RUN\\s+([a-z0-9-]+)((?:\\s+[a-z_]+=\\S+)*)\\s*");
  private static final Pattern NAMED =
      Pattern.compile("(?is)(LOGS|CANCEL|STATUS)\\s+([a-z0-9-]+)((?:\\s+[a-z_]+=\\S+)*)\\s*");
  private static final Logger LOG = LoggerFactory.getLogger("launcher");
  private static final Duration KEEP_FINISHED = Duration.ofHours(24);
  private static final Pattern VALUE = Pattern.compile("[A-Za-z0-9_.,:/=+-]{1,200}");
  private static final String MANAGED = "app.kubernetes.io/managed-by";
  private static final String JOB_LABEL = "duck-kyuubi/job";
  private static final String USER_LABEL = "duck-kyuubi/user";

  // Created on first use: Kyuubi instantiates every registered dialect to find the one a session
  // asks for, so a duckdb session outside Kubernetes must not need the service-account files.
  private Kube kube;
  private final long pollMillis;

  public LauncherDialect() {
    this(null, 3000);
  }

  /** Tests: a given API client, and how often a waiting RUN polls its pod. */
  LauncherDialect(Kube kube, long pollMillis) {
    this.kube = kube;
    this.pollMillis = pollMillis;
  }

  private synchronized Kube kube() {
    if (kube == null) {
      kube = new Kube();
    }
    return kube;
  }

  @Override
  public String name() {
    return "launcher";
  }

  @Override
  public Statement createStatement(Connection connection, int fetchSize) {
    String user = LauncherConnectionProvider.USERS.get(connection);
    Statement statement;
    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    AtomicReference<String> running = new AtomicReference<>();   // the pod a RUN is waiting on
    return (Statement) Proxy.newProxyInstance(
        getClass().getClassLoader(),
        new Class<?>[] {Statement.class},
        (proxy, method, args) -> {
          if (method.getName().equals("cancel")) {
            String pod = running.get();
            if (pod != null) {
              deletePod(user, pod);
            }
          }
          if (SQL_METHODS.contains(method.getName()) && args != null && args[0] instanceof String) {
            args[0] = answer(user, (String) args[0], running);   // our own VALUES query, never user SQL
          }
          try {
            return method.invoke(statement, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  // ------------------------------------------------------------------ commands

  private String answer(String user, String sql, AtomicReference<String> running) throws SQLException {
    if (user == null) {
      throw new SQLException("launcher: this session has no Kyuubi user");
    }
    String s = sql.strip();
    while (s.endsWith(";")) {
      s = s.substring(0, s.length() - 1).strip();
    }
    try {
      String upper = s.toUpperCase(Locale.ROOT);
      if (upper.equals("SELECT 1")) {
        return "SELECT 1";   // the JDBC engine's start-up check (kyuubi.engine.jdbc.initialize.sql)
      }
      if (upper.equals("SHOW JOBS")) {
        return showJobs();
      }
      if (upper.equals("SHOW RUNS")) {
        return showRuns(user);
      }
      Matcher named = NAMED.matcher(s);
      if (named.matches()) {
        String verb = named.group(1).toUpperCase(Locale.ROOT);
        switch (verb) {
          case "LOGS":
            return logs(user, named.group(2), options(named.group(3)));
          case "STATUS":
            return status(user, named.group(2));
          default:
            return cancel(user, named.group(2));
        }
      }
      Matcher run = RUN.matcher(s);
      if (run.matches()) {
        return run(user, run.group(1).toLowerCase(Locale.ROOT), run.group(2).strip(), running);
      }
    } catch (IOException e) {
      throw new SQLException("launcher: " + e.getMessage(), e);
    }
    throw new SQLException("launcher sessions accept only: SHOW JOBS | RUN <job> [name=value ...] | "
        + "SHOW RUNS | LOGS <run> | CANCEL <run>");
  }

  private String showJobs() throws IOException {
    List<List<Object>> rows = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> it = catalog().fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> job = it.next();
      rows.add(List.of(job.getKey(), job.getValue().path("description").asText(""),
          job.getValue().path("image").asText(), job.getValue().path("params").toString()));
    }
    return values(List.of("job", "description", "image", "params"), rows);
  }

  private String showRuns(String user) throws IOException {
    JsonNode pods = kube().json("GET", "/api/v1/namespaces/" + ns(user) + "/pods?labelSelector="
        + Kube.q(MANAGED + "=kyuubi-launcher"), null);
    List<List<Object>> rows = new ArrayList<>();
    for (JsonNode p : pods.path("items")) {
      rows.add(List.of(p.path("metadata").path("name").asText(),
          p.path("metadata").path("labels").path(JOB_LABEL).asText(),
          p.path("status").path("phase").asText(), p.path("metadata").path("creationTimestamp").asText()));
    }
    return values(List.of("run", "job", "phase", "started"), rows);
  }

  private String logs(String user, String run, Map<String, String> opts) throws IOException, SQLException {
    ownRun(user, run);
    int from = 1;
    try {
      from = Math.max(1, Integer.parseInt(opts.getOrDefault("from", "1")));
    } catch (NumberFormatException e) {
      throw new SQLException("launcher: from must be a line number");
    }
    String[] lines = podLog(user, run, 0);
    List<List<Object>> rows = new ArrayList<>();
    for (int n = from; n <= lines.length && rows.size() < 2000; n++) {
      rows.add(List.of(n, lines[n - 1]));
    }
    return values(List.of("n", "line"), rows);
  }

  private String status(String user, String run) throws IOException {
    JsonNode pod = ownRun(user, run);
    JsonNode st = pod.path("status");
    String phase = pod.path("metadata").has("deletionTimestamp") ? "Terminating" : st.path("phase").asText();
    JsonNode term = st.path("containerStatuses").path(0).path("state").path("terminated");
    int exit = term.path("exitCode").asInt(-1);
    Instant started = Instant.parse(pod.path("metadata").path("creationTimestamp").asText());
    Instant ended = term.has("finishedAt") ? Instant.parse(term.path("finishedAt").asText()) : Instant.now();
    double seconds = Math.round(Duration.between(started, ended).toMillis() / 100.0) / 10.0;
    String summary = "";
    String message = st.path("containerStatuses").path(0).path("state").path("waiting").path("reason").asText("");
    boolean done = phase.equals("Succeeded") || phase.equals("Failed");
    if (done) {
      String[] lines = podLog(user, run, 200);
      summary = lastJsonLine(String.join("\n", lines));
      String meaning = catalog().path(pod.path("metadata").path("labels").path(JOB_LABEL).asText())
          .path("exit_codes").path(String.valueOf(exit)).asText("");
      message = phase.equals("Failed") && !meaning.isEmpty() ? meaning : message;
    }
    return values(List.of("run", "job", "phase", "exit_code", "seconds", "summary", "message"),
        List.of(List.of(run, pod.path("metadata").path("labels").path(JOB_LABEL).asText(), phase, exit, seconds,
            summary, message)));
  }

  /** All of a run's log (at most 4 MB), or its last `tail` lines when tail > 0; empty while the pod hasn't started. */
  private String[] podLog(String user, String run, int tail) throws IOException {
    String path = "/api/v1/namespaces/" + ns(user) + "/pods/" + run + "/log?limitBytes=4000000"
        + (tail > 0 ? "&tailLines=" + tail : "");
    try {
      String log = kube().text(path);
      return log.isEmpty() ? new String[0] : log.stripTrailing().split("\n", -1);
    } catch (IOException e) {
      if (e.getMessage().contains("HTTP 400")) {
        return new String[0];   // container is waiting to start
      }
      throw e;
    }
  }

  private static Map<String, String> options(String text) {
    Map<String, String> opts = new LinkedHashMap<>();
    for (String kv : text.strip().isEmpty() ? new String[0] : text.strip().split("\\s+")) {
      String[] p = kv.split("=", 2);
      opts.put(p[0].toLowerCase(Locale.ROOT), p[1]);
    }
    return opts;
  }

  private String cancel(String user, String run) throws IOException {
    ownRun(user, run);
    deletePod(user, run);
    return values(List.of("run", "status"), List.of(List.of(run, "cancelled")));
  }

  // ------------------------------------------------------------------ RUN

  private String run(String user, String jobName, String argText, AtomicReference<String> running)
      throws IOException, SQLException {
    JsonNode job = catalog().path(jobName);
    if (job.isMissingNode()) {
      throw new SQLException("launcher: no job " + jobName + " in the catalog (SHOW JOBS)");
    }
    Map<String, String> given = options(argText);
    boolean async = "true".equalsIgnoreCase(given.remove("async"));   // a launcher keyword, not a job parameter
    Params params = Params.check(job.path("params"), given);
    sweep(user);
    String run = jobName + "-" + UUID.randomUUID().toString().substring(0, 8);
    ObjectNode pod = podSpec(user, jobName, run, job, params);
    kube().json("POST", "/api/v1/namespaces/" + ns(user) + "/pods", pod);
    if (async) {
      return values(List.of("run", "job", "status"), List.of(List.of(run, jobName, "Submitted")));
    }
    running.set(run);
    long t0 = System.nanoTime();
    try {
      String phase = waitFor(user, run, params.timeoutSeconds(job));
      JsonNode status = kube().json("GET", "/api/v1/namespaces/" + ns(user) + "/pods/" + run, null).path("status");
      int exit = status.path("containerStatuses").path(0).path("state").path("terminated").path("exitCode").asInt(-1);
      String log = kube().text("/api/v1/namespaces/" + ns(user) + "/pods/" + run + "/log?tailLines=200");
      String summary = lastJsonLine(log);
      double seconds = Math.round((System.nanoTime() - t0) / 1e8) / 10.0;
      if (!phase.equals("Succeeded")) {
        String meaning = job.path("exit_codes").path(String.valueOf(exit)).asText("");
        throw new SQLException("launcher: run " + run + " ended " + phase + " with exit code " + exit
            + (meaning.isEmpty() ? "" : " (" + meaning + ")") + "\n" + tail(log, 25));
      }
      return values(List.of("run", "job", "status", "exit_code", "seconds", "summary", "log_tail"),
          List.of(List.of(run, jobName, phase, exit, seconds, summary, tail(log, 15))));
    } finally {
      running.set(null);
    }
  }

  static ObjectNode podSpec(String user, String jobName, String run, JsonNode job, Params params) {
    ObjectNode pod = Kube.JSON.createObjectNode();
    pod.put("apiVersion", "v1").put("kind", "Pod");
    ObjectNode meta = pod.putObject("metadata");
    meta.put("name", run);
    ObjectNode labels = meta.putObject("labels");
    labels.put(MANAGED, "kyuubi-launcher").put(JOB_LABEL, jobName).put(USER_LABEL, user);
    ObjectNode spec = pod.putObject("spec");
    spec.put("restartPolicy", "Never");
    spec.put("automountServiceAccountToken", false);   // the job needs no Kubernetes API access
    spec.put("activeDeadlineSeconds", params.timeoutSeconds(job));
    if (params.nodeSelector != null) {
      spec.putObject("nodeSelector").put("kubernetes.io/hostname", params.nodeSelector);
    }
    ObjectNode c = spec.putArray("containers").addObject();
    // Always by default, so a rebuilt image that reuses its tag is picked up; IfNotPresent for preloaded images
    c.put("name", "job").put("image", job.path("image").asText())
        .put("imagePullPolicy", job.path("image_pull_policy").asText("Always"));
    if (job.has("command")) {
      ArrayNode command = c.putArray("command");
      job.path("command").forEach(a -> command.add(a.asText()));
    }
    ArrayNode cmd = c.putArray("args");
    job.path("args").forEach(a -> cmd.add(a.asText()));
    params.args.forEach(cmd::add);
    ArrayNode envFrom = c.putArray("envFrom");
    job.path("secrets").forEach(s -> envFrom.addObject().putObject("secretRef").put("name", s.asText()));
    ArrayNode env = c.putArray("env");
    env.addObject().put("name", "KYUUBI_USER").put("value", user);
    env.addObject().put("name", "LAUNCHER_RUN").put("value", run);
    params.env.forEach((k, v) -> env.addObject().put("name", k).put("value", v));
    ObjectNode res = c.putObject("resources");
    // cpu is what the job may use (its CPU limit: the cgroup quota, which a job can read to size itself);
    // the request stays small so a 4-core job still fits on a 4-core node beside system pods.
    res.putObject("requests").put("cpu", params.cpuRequest).put("memory", params.memory);
    res.putObject("limits").put("cpu", params.cpu).put("memory", params.memory);
    c.putObject("securityContext").put("allowPrivilegeEscalation", false).put("runAsNonRoot", true)
        .putObject("capabilities").putArray("drop").add("ALL");
    return pod;
  }

  private String waitFor(String user, String run, long timeoutSeconds) throws IOException, SQLException {
    long started = System.currentTimeMillis();
    long deadline = started + (timeoutSeconds + 120) * 1000;
    int[] emitted = {0};   // lines of the pod's log already written to the engine's log
    while (System.currentTimeMillis() < deadline) {
      JsonNode status;
      try {
        status = kube().json("GET", "/api/v1/namespaces/" + ns(user) + "/pods/" + run, null).path("status");
      } catch (IOException e) {
        if (e.getMessage().contains("HTTP 404")) {
          throw new SQLException("launcher: run " + run + " was cancelled (its pod is gone)");
        }
        throw e;
      }
      String phase = status.path("phase").asText();
      if (!phase.equals("Pending")) {
        stream(user, run, emitted);   // the pod's new log lines, into the engine's log
      }
      if (phase.equals("Succeeded") || phase.equals("Failed")) {
        return phase;
      }
      for (JsonNode cond : status.path("conditions")) {
        if (cond.path("type").asText().equals("PodScheduled") && cond.path("reason").asText().equals("Unschedulable")
            && System.currentTimeMillis() - started > 60_000) {
          deletePod(user, run);
          throw new SQLException("launcher: run " + run + " can't be scheduled: " + cond.path("message").asText()
              + " (lower cpu or memory, or try another node)");
        }
      }
      String waiting = status.path("containerStatuses").path(0).path("state").path("waiting").path("reason").asText("");
      if (waiting.matches("ErrImagePull|ImagePullBackOff|CreateContainerConfigError|InvalidImageName")) {
        deletePod(user, run);
        throw new SQLException("launcher: run " + run + " can't start: " + waiting);
      }
      try {
        Thread.sleep(pollMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        deletePod(user, run);
        throw new SQLException("launcher: interrupted; run " + run + " cancelled");
      }
    }
    deletePod(user, run);
    throw new SQLException("launcher: run " + run + " timed out and was deleted");
  }

  // ------------------------------------------------------------------ helpers

  /** Writes the run's log lines not yet written to the engine's log. */
  private void stream(String user, String run, int[] emitted) {
    try {
      String[] lines = podLog(user, run, 0);
      for (; emitted[0] < lines.length; emitted[0]++) {
        LOG.info("[{}] {}", run, lines[emitted[0]]);
      }
    } catch (IOException | RuntimeException e) {
      LOG.warn("launcher: reading the log of {} failed: {}", run, e.getMessage());
    }
  }

  private JsonNode catalog() throws IOException {
    return Kube.JSON.readTree(Files.readString(CATALOG));   // re-read: ConfigMap updates apply at once
  }

  private static String ns(String user) {
    return "spark-" + user;
  }

  private JsonNode ownRun(String user, String run) throws IOException {
    JsonNode pod = kube().json("GET", "/api/v1/namespaces/" + ns(user) + "/pods/" + run, null);
    if (!"kyuubi-launcher".equals(pod.path("metadata").path("labels").path(MANAGED).asText())) {
      throw new IOException(run + " is not a launcher run");
    }
    return pod;
  }

  /** Finished run pods are kept for a day (for STATUS and LOGS), then removed. */
  private void sweep(String user) {
    try {
      JsonNode pods = kube().json("GET", "/api/v1/namespaces/" + ns(user) + "/pods?labelSelector="
          + Kube.q(MANAGED + "=kyuubi-launcher"), null);
      for (JsonNode p : pods.path("items")) {
        String phase = p.path("status").path("phase").asText();
        if ((phase.equals("Succeeded") || phase.equals("Failed"))
            && Duration.between(Instant.parse(p.path("metadata").path("creationTimestamp").asText()), Instant.now())
                .compareTo(KEEP_FINISHED) > 0) {
          deletePod(user, p.path("metadata").path("name").asText());
        }
      }
    } catch (IOException | RuntimeException e) {
      LOG.warn("launcher: sweeping old runs failed: {}", e.getMessage());
    }
  }

  private void deletePod(String user, String run) {
    try {
      kube().json("DELETE", "/api/v1/namespaces/" + ns(user) + "/pods/" + run + "?gracePeriodSeconds=5", null);
    } catch (IOException ignored) {
      // already gone
    }
  }

  private static String lastJsonLine(String log) {
    String found = "";
    for (String line : log.split("\n")) {
      if (line.startsWith("{") && line.endsWith("}")) {
        found = line;
      }
    }
    return found;
  }

  private static String tail(String log, int n) {
    String[] lines = log.strip().split("\n");
    return String.join("\n", java.util.Arrays.copyOfRange(lines, Math.max(0, lines.length - n), lines.length));
  }

  /** SELECT * FROM (VALUES ...) t(cols): the answer as a real result set, from literals we quote ourselves. */
  static String values(List<String> cols, List<List<Object>> rows) {
    if (rows.isEmpty()) {
      StringBuilder empty = new StringBuilder("SELECT ");
      for (int i = 0; i < cols.size(); i++) {
        empty.append(i == 0 ? "" : ", ").append("NULL::VARCHAR AS ").append(cols.get(i));
      }
      return empty.append(" WHERE false").toString();
    }
    StringBuilder sql = new StringBuilder("SELECT * FROM (VALUES ");
    for (int r = 0; r < rows.size(); r++) {
      sql.append(r == 0 ? "(" : ", (");
      List<Object> row = rows.get(r);
      for (int i = 0; i < row.size(); i++) {
        Object v = row.get(i);
        sql.append(i == 0 ? "" : ", ").append(v instanceof Number ? v.toString()
            : "'" + String.valueOf(v).replace("'", "''") + "'");
      }
      sql.append(")");
    }
    return sql.append(") t(").append(String.join(", ", cols)).append(")").toString();
  }

  @Override
  public JdbcTRowSetGenerator getTRowSetGenerator() {
    return new DefaultJdbcTRowSetGenerator();
  }

  @Override
  public SchemaHelper getSchemaHelper() {
    return new SchemaHelper() {};
  }

  /** A RUN's parameters, checked against the job's declaration. */
  static final class Params {
    String cpu = "1";
    String cpuRequest = "500m";
    String memory = "1Gi";
    String nodeSelector;
    final List<String> args = new ArrayList<>();
    final Map<String, String> env = new LinkedHashMap<>();
    Long timeout;

    long timeoutSeconds(JsonNode job) {
      return timeout != null ? timeout : job.path("timeout_s").asLong(3600);
    }

    static Params check(JsonNode decl, Map<String, String> given) throws SQLException {
      for (String k : given.keySet()) {
        if (!decl.has(k)) {
          throw new SQLException("launcher: unknown parameter " + k + "; this job takes " + fieldNames(decl));
        }
        if (!VALUE.matcher(given.get(k)).matches()) {
          throw new SQLException("launcher: parameter " + k + " has characters that aren't allowed");
        }
      }
      Params p = new Params();
      Iterator<Map.Entry<String, JsonNode>> it = decl.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        String name = e.getKey();
        JsonNode d = e.getValue();
        String v = given.containsKey(name) ? given.get(name) : d.path("default").isMissingNode() ? null : d.path("default").asText();
        if (v == null) {
          continue;
        }
        switch (d.path("type").asText("string")) {
          case "cores":
            double cores = number(name, v);
            range(name, cores, d);
            p.cpu = v;
            break;
          case "size":
            long bytes = bytes(name, v);
            if ((d.has("min") && bytes < bytes(name, d.get("min").asText()))
                || (d.has("max") && bytes > bytes(name, d.get("max").asText()))) {
              throw new SQLException("launcher: " + name + " must be between " + d.path("min").asText("0")
                  + " and " + d.path("max").asText("any"));
            }
            p.memory = v;
            break;
          case "int":
            range(name, number(name, v), d);
            break;
          case "enum":
            boolean ok = false;
            for (JsonNode a : d.path("values")) {
              ok |= a.asText().equals(v);
            }
            if (!ok) {
              throw new SQLException("launcher: " + name + " must be one of " + d.path("values"));
            }
            break;
          default:
            break;
        }
        String value = d.has("map") && d.path("map").has(v) ? d.path("map").path(v).asText() : v;
        switch (d.path("to").asText("arg")) {
          case "node":
            p.nodeSelector = value.isEmpty() ? null : value;   // "any": no selector
            break;
          case "timeout":
            p.timeout = (long) number(name, value);
            break;
          case "resources":
            break;
          case "env":
            p.env.put(d.path("env").asText(), value);
            break;
          default:
            if (d.has("arg")) {
              p.args.add(d.path("arg").asText());
              p.args.add(value);
            }
        }
      }
      // DuckDB's limits follow the pod's: 3/4 of memory, threads = cores (for jobs that run DuckDB)
      p.env.putIfAbsent("DUCKDB_MEMORY_LIMIT", (bytes("memory", p.memory) * 3 / 4 / (1 << 20)) + "MB");
      p.env.putIfAbsent("DUCKDB_THREADS", String.valueOf(Math.max(1, (int) Math.ceil(number("cpu", p.cpu)))));
      return p;
    }

    private static String fieldNames(JsonNode decl) {
      List<String> names = new ArrayList<>();
      decl.fieldNames().forEachRemaining(names::add);
      return names.toString();
    }

    private static double number(String name, String v) throws SQLException {
      try {
        return Double.parseDouble(v);
      } catch (NumberFormatException e) {
        throw new SQLException("launcher: " + name + " must be a number");
      }
    }

    private static void range(String name, double v, JsonNode d) throws SQLException {
      range(name, v, d.has("min") ? d.get("min").asDouble() : null, d.has("max") ? d.get("max").asDouble() : null);
    }

    private static void range(String name, double v, Number min, Number max) throws SQLException {
      if ((min != null && v < min.doubleValue()) || (max != null && v > max.doubleValue())) {
        throw new SQLException("launcher: " + name + " must be between " + min + " and " + max);
      }
    }

    static long bytes(String name, String v) throws SQLException {
      Matcher m = Pattern.compile("(\\d+)(Ki|Mi|Gi)").matcher(v);
      if (!m.matches()) {
        throw new SQLException("launcher: " + name + " must look like 512Mi or 2Gi");
      }
      long n = Long.parseLong(m.group(1));
      return n << (m.group(2).equals("Ki") ? 10 : m.group(2).equals("Mi") ? 20 : 30);
    }
  }
}
