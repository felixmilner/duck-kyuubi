package io.github.felixmilner.duckkyuubi.launcher;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The few Kubernetes API calls the launcher needs, over HTTPS with the pod's service-account token
 * (Kyuubi's service account; the Role in each spark-<user> lets it manage pods there). No client
 * library: the JDBC engine's classpath has Jackson, which is enough.
 */
class Kube {
  private static final String API = "https://kubernetes.default.svc";
  private static final Path SA = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");
  static final ObjectMapper JSON = new ObjectMapper();

  /** Where each call's bearer token comes from. */
  interface Token {
    String get() throws IOException;
  }

  private final String api;
  private final HttpClient http;
  private final Token token;

  /** In the cluster: the API server, its CA and the pod's service-account token. */
  Kube() {
    this(API, inClusterClient(),
        // read each time: projected service-account tokens are rotated
        () -> Files.readString(SA.resolve("token")).strip());
  }

  /** Any API server, e.g. a fake one in tests. */
  Kube(String api, HttpClient http, Token token) {
    this.api = api;
    this.http = http;
    this.token = token;
  }

  private static HttpClient inClusterClient() {
    try {
      KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
      trust.load(null, null);
      try (InputStream in = Files.newInputStream(SA.resolve("ca.crt"))) {
        int i = 0;
        for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
          trust.setCertificateEntry("ca" + i++, c);
        }
      }
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trust);
      SSLContext ssl = SSLContext.getInstance("TLS");
      ssl.init(null, tmf.getTrustManagers(), null);
      return HttpClient.newBuilder().sslContext(ssl).connectTimeout(Duration.ofSeconds(10)).build();
    } catch (Exception e) {
      throw new IllegalStateException("launcher: can't set up the Kubernetes API client", e);
    }
  }

  JsonNode json(String method, String path, JsonNode body) throws IOException {
    String text = call(method, path, body == null ? null : JSON.writeValueAsString(body), "application/json");
    return JSON.readTree(text);
  }

  String text(String path) throws IOException {
    return call("GET", path, null, null);
  }

  static String q(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private String call(String method, String path, String body, String type) throws IOException {
    HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(api + path))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", "Bearer " + token.get())
        .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body));
    if (type != null) {
      req.header("Content-Type", type);
    }
    try {
      HttpResponse<String> resp = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() / 100 != 2) {
        String msg = resp.body();
        try {
          msg = JSON.readTree(msg).path("message").asText(msg);
        } catch (IOException ignored) {
          // not JSON: keep the raw body
        }
        throw new IOException("Kubernetes API " + method + " " + path.split("\\?")[0] + ": HTTP "
            + resp.statusCode() + ": " + msg);
      }
      return resp.body();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted", e);
    }
  }
}
