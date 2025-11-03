package com.example.enterprise_kafka_starter.jdbc;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

final class JsonProjector {
  private static final ObjectMapper M = new ObjectMapper();
  private JsonProjector(){}

  /** Parse arbitrary JSON to a flat Map<String,Object>. */
  static Map<String,Object> toRow(String json) {
    try {
      JsonNode n = M.readTree(json);
      Map<String,Object> out = new HashMap<>();
      n.fieldNames().forEachRemaining(fn -> {
        String snake = toSnake(fn);
        out.put(snake, jsonToJava(n.get(fn)));
      });
      return out;
    } catch (Exception e) {
      throw new RuntimeException("Invalid JSON: " + json, e);
    }
  }

  private static String toSnake(String s) {
    return s.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(Locale.ROOT);
  }

  /** Order and coerce values to match table columns. */
  static Object[] projectToColumns(Map<String,Object> row, List<TableMeta.Column> cols) {
    Object[] arr = new Object[cols.size()];
    for (int i = 0; i < cols.size(); i++) {
      TableMeta.Column c = cols.get(i);
      Object v = row.get(c.name); // case-sensitive: align your JSON field names to table cols
      arr[i] = coerce(v, c.dbxType);
    }
    return arr;
  }

  private static Object jsonToJava(JsonNode j) {
    if (j == null || j.isNull()) return null;
    if (j.isTextual()) return j.asText();
    if (j.isInt() || j.isLong()) return j.asLong(); // safe upcast
    if (j.isFloat() || j.isDouble() || j.isBigDecimal()) return j.asDouble();
    if (j.isBoolean()) return j.asBoolean();
    // For objects/arrays, store JSON string (works with VARCHAR columns)
    return j.toString();
  }

  private static Object coerce(Object v, String dbxType) {
    if (v == null) return null;
    String t = dbxType.toUpperCase(Locale.ROOT);
    int paren = t.indexOf('(');
    if (paren > 0) t = t.substring(0, paren);

    try {
      switch (t) {
        case "STRING": case "CHAR": case "VARCHAR":
          return v.toString();
        case "BOOLEAN":
          if (v instanceof Boolean) return v;
          return Boolean.valueOf(v.toString());
        case "TINYINT": case "SMALLINT": case "INT": case "INTEGER":
          if (v instanceof Number) return ((Number) v).intValue();
          return Integer.valueOf(v.toString());
        case "BIGINT":
          if (v instanceof Number) return ((Number) v).longValue();
          return Long.valueOf(v.toString());
        case "FLOAT": case "DOUBLE": case "DECIMAL":
          if (v instanceof Number) return ((Number) v).doubleValue();
          return Double.valueOf(v.toString());
        case "DATE":
          // accept "yyyy-MM-dd" or epoch millis
          if (v instanceof Number) return new java.sql.Date(((Number) v).longValue());
          return java.sql.Date.valueOf(v.toString());
        case "TIMESTAMP":
        case "TIMESTAMP_NTZ":
          // accept ISO-8601 or epoch millis
          if (v instanceof Number) return new Timestamp(((Number) v).longValue());
          Instant ins = Instant.parse(v.toString()); // "2024-10-21T10:20:30Z"
          return Timestamp.from(ins);
        default:
          // Fallback: store as string
          return v.toString();
      }
    } catch (Exception ex) {
      // If coercion fails, fall back to string to avoid crashing ingestion
      return v.toString();
    }
  }
}
