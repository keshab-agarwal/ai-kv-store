package com.example.kv;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class KvHttpYcsbClient extends DB {
  private String[] endpoints;
  private volatile String leader;

  @Override
  public void init() throws DBException {
    Properties p = getProperties();
    String endpointCsv = p.getProperty("kv.endpoints", "http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003");
    this.endpoints = endpointCsv.split(",");
    this.leader = endpoints[0].trim();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String k = keyHex128(key);
    for (int i = 0; i < endpoints.length + 1; i++) {
      String target = i == 0 ? leader : endpoints[(i - 1) % endpoints.length].trim();
      try {
        HttpURLConnection conn = open("GET", target + "/kv/" + k);
        int code = conn.getResponseCode();
        String body = readBody(conn, code);
        if (code == 200 && body.contains("\"FOUND\"")) {
          String v = extractJsonString(body, "value");
          if (v == null) {
            return Status.ERROR;
          }
          result.put("value", new ByteArrayByteIterator(Base64.getDecoder().decode(v)));
          return Status.OK;
        }
        if (code == 200 && body.contains("\"NOT_FOUND\"")) {
          return Status.NOT_FOUND;
        }
        String maybeLeader = extractJsonString(body, "leader");
        if (code == 307 && maybeLeader != null && !maybeLeader.isEmpty()) {
          leader = maybeLeader;
          continue;
        }
      } catch (Exception e) {
        // continue rotating endpoints
      }
    }
    return Status.SERVICE_UNAVAILABLE;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return putLike(key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return putLike(key, values);
  }

  @Override
  public Status delete(String table, String key) {
    String k = keyHex128(key);
    for (int i = 0; i < endpoints.length + 1; i++) {
      String target = i == 0 ? leader : endpoints[(i - 1) % endpoints.length].trim();
      try {
        HttpURLConnection conn = open("DELETE", target + "/kv/" + k);
        int code = conn.getResponseCode();
        String body = readBody(conn, code);
        if (code == 200 && body.contains("\"OK\"")) {
          return Status.OK;
        }
        String maybeLeader = extractJsonString(body, "leader");
        if (code == 307 && maybeLeader != null && !maybeLeader.isEmpty()) {
          leader = maybeLeader;
          continue;
        }
      } catch (Exception e) {
        // continue
      }
    }
    return Status.SERVICE_UNAVAILABLE;
  }

  private Status putLike(String key, Map<String, ByteIterator> values) {
    String k = keyHex128(key);
    byte[] payload = toSingleValue(values);
    String body = "{\"value\":\"" + Base64.getEncoder().encodeToString(payload) + "\"}";

    for (int i = 0; i < endpoints.length + 1; i++) {
      String target = i == 0 ? leader : endpoints[(i - 1) % endpoints.length].trim();
      try {
        HttpURLConnection conn = open("PUT", target + "/kv/" + k);
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
          os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        int code = conn.getResponseCode();
        String resp = readBody(conn, code);
        if (code == 200 && resp.contains("\"OK\"")) {
          return Status.OK;
        }
        String maybeLeader = extractJsonString(resp, "leader");
        if (code == 307 && maybeLeader != null && !maybeLeader.isEmpty()) {
          leader = maybeLeader;
          continue;
        }
      } catch (Exception e) {
        // continue
      }
    }
    return Status.SERVICE_UNAVAILABLE;
  }

  private static byte[] toSingleValue(Map<String, ByteIterator> values) {
    if (values == null || values.isEmpty()) {
      return new byte[0];
    }
    ByteIterator v = values.get("value");
    if (v != null) {
      return v.toArray();
    }
    Map.Entry<String, ByteIterator> first = values.entrySet().iterator().next();
    return first.getValue().toArray();
  }

  private static HttpURLConnection open(String method, String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setConnectTimeout(800);
    conn.setReadTimeout(2000);
    conn.setRequestMethod(method);
    conn.setRequestProperty("Content-Type", "application/json");
    return conn;
  }

  private static String readBody(HttpURLConnection conn, int code) throws IOException {
    byte[] bytes;
    if (code >= 200 && code < 400) {
      bytes = conn.getInputStream().readAllBytes();
    } else {
      bytes = conn.getErrorStream() != null ? conn.getErrorStream().readAllBytes() : new byte[0];
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static String keyHex128(String key) {
    if (key.matches("^[0-9a-fA-F]{32}$")) {
      return key.toLowerCase();
    }
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] d = md.digest(key.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : d) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("md5 missing", e);
    }
  }

  private static String extractJsonString(String body, String key) {
    String needle = "\"" + key + "\":\"";
    int i = body.indexOf(needle);
    if (i < 0) {
      return null;
    }
    int start = i + needle.length();
    int end = body.indexOf('"', start);
    if (end < 0) {
      return null;
    }
    return body.substring(start, end);
  }
}
