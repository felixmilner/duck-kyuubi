package io.github.felixmilner.duckkyuubi.launcher;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Just enough of the Kubernetes API for the launcher: create, get, list and delete pods, and read a pod's
 * log, in any namespace. Tests script what happens to a pod with onCreate and onGet.
 */
final class FakeKubernetes implements AutoCloseable {
  static final String MANAGED = "app.kubernetes.io/managed-by";

  /** "namespace/name" to pod. */
  final Map<String, ObjectNode> pods = new ConcurrentHashMap<>();
  final Map<String, String> logs = new ConcurrentHashMap<>();
  /** "METHOD path?query" of every request, and the Authorization header each one had. */
  final List<String> requests = new CopyOnWriteArrayList<>();
  final List<String> authorizations = new CopyOnWriteArrayList<>();
  /** Runs on each new pod, which starts out Pending. */
  volatile Consumer<ObjectNode> onCreate = pod -> { };
  /** Runs on a pod before each GET of it, e.g. to move it along. */
  volatile Consumer<ObjectNode> onGet = pod -> { };
  private final Map<String, int[]> failures = new ConcurrentHashMap<>();
  private final Map<String, String> failureMessages = new ConcurrentHashMap<>();

  private final HttpServer server;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  FakeKubernetes() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/api/v1/namespaces/", this::handle);
    server.setExecutor(executor);
    server.start();
  }

  Kube kube() {
    return new Kube("http://127.0.0.1:" + server.getAddress().getPort(), HttpClient.newHttpClient(),
        () -> "test-token");
  }

  /** Answers every `method` request with `status` and `message` from now on. */
  void fail(String method, int status, String message) {
    failures.put(method, new int[] {status});
    failureMessages.put(method, message);
  }

  // ------------------------------------------------------------------ pods for tests

  /** A pod as the launcher would have created it, `age` ago. */
  ObjectNode put(String namespace, String name, String job, String phase, Duration age) {
    ObjectNode pod = Kube.JSON.createObjectNode();
    ObjectNode meta = pod.putObject("metadata");
    meta.put("name", name).put("creationTimestamp", Instant.now().minus(age).toString());
    if (job != null) {
      meta.putObject("labels").put(MANAGED, "kyuubi-launcher").put("duck-kyuubi/job", job);
    }
    pod.putObject("status").put("phase", phase);
    pods.put(namespace + "/" + name, pod);
    return pod;
  }

  static ObjectNode terminated(ObjectNode pod, String phase, int exitCode) {
    ObjectNode status = pod.putObject("status").put("phase", phase);
    status.putArray("containerStatuses").addObject().putObject("state").putObject("terminated")
        .put("exitCode", exitCode).put("finishedAt", Instant.now().toString());
    return pod;
  }

  static ObjectNode waiting(ObjectNode pod, String reason) {
    ObjectNode status = pod.putObject("status").put("phase", "Pending");
    status.putArray("containerStatuses").addObject().putObject("state").putObject("waiting").put("reason", reason);
    return pod;
  }

  void log(String namespace, ObjectNode pod, String text) {
    logs.put(namespace + "/" + pod.path("metadata").path("name").asText(), text);
  }

  long count(String request) {
    return requests.stream().filter(r -> r.startsWith(request)).count();
  }

  // ------------------------------------------------------------------ the API

  private void handle(HttpExchange ex) throws IOException {
    try {
      String method = ex.getRequestMethod();
      String path = ex.getRequestURI().getPath();
      String query = ex.getRequestURI().getRawQuery();
      requests.add(method + " " + path + (query == null ? "" : "?" + query));
      authorizations.add(ex.getRequestHeaders().getFirst("Authorization"));
      if (failures.containsKey(method)) {
        send(ex, failures.get(method)[0], status(failureMessages.get(method)));
        return;
      }
      // /api/v1/namespaces/{ns}/pods[/{name}[/log]]
      List<String> parts = Arrays.asList(path.split("/"));
      String ns = parts.get(4);
      if (parts.size() == 6 && method.equals("POST")) {
        ObjectNode pod = (ObjectNode) Kube.JSON.readTree(ex.getRequestBody());
        ((ObjectNode) pod.path("metadata")).put("creationTimestamp", Instant.now().toString());
        pod.putObject("status").put("phase", "Pending");
        onCreate.accept(pod);
        pods.put(ns + "/" + pod.path("metadata").path("name").asText(), pod);
        send(ex, 201, pod.toString());
      } else if (parts.size() == 6 && method.equals("GET")) {
        boolean launcherOnly = query != null
            && URLDecoder.decode(query, StandardCharsets.UTF_8).contains(MANAGED + "=kyuubi-launcher");
        ObjectNode list = Kube.JSON.createObjectNode();
        ArrayNode items = list.putArray("items");
        pods.forEach((key, pod) -> {
          if (key.startsWith(ns + "/")
              && (!launcherOnly || pod.path("metadata").path("labels").path(MANAGED).asText().equals("kyuubi-launcher"))) {
            items.add(pod);
          }
        });
        send(ex, 200, list.toString());
      } else {
        String key = ns + "/" + parts.get(6);
        ObjectNode pod = pods.get(key);
        if (pod == null) {
          send(ex, 404, status("pods \"" + parts.get(6) + "\" not found"));
        } else if (parts.size() == 8) {
          String log = logs.get(key);
          if (log == null) {
            send(ex, 400, status("container \"job\" in pod \"" + parts.get(6) + "\" is waiting to start"));
          } else {
            send(ex, 200, tail(log, query));
          }
        } else if (method.equals("DELETE")) {
          pods.remove(key);
          send(ex, 200, pod.toString());
        } else {
          onGet.accept(pod);
          send(ex, 200, pod.toString());
        }
      }
    } catch (RuntimeException e) {
      send(ex, 500, status(e.toString()));
    } finally {
      ex.close();
    }
  }

  private static String tail(String log, String query) {
    if (query == null || !query.contains("tailLines=")) {
      return log;
    }
    int n = Integer.parseInt(query.replaceAll(".*tailLines=(\\d+).*", "$1"));
    String[] lines = log.split("\n");
    return String.join("\n", Arrays.copyOfRange(lines, Math.max(0, lines.length - n), lines.length)) + "\n";
  }

  private static String status(String message) {
    return Kube.JSON.createObjectNode().put("kind", "Status").put("message", message).toString();
  }

  private static void send(HttpExchange ex, int code, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(code, bytes.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(bytes);
    }
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
