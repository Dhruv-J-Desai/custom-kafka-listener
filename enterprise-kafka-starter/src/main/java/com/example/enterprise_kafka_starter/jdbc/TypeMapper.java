// src/main/java/com/example/enterprise_kafka_starter/jdbc/TypeMapper.java
package com.example.enterprise_kafka_starter.jdbc;

import java.sql.Types;
import java.util.Locale;

final class TypeMapper {
  private TypeMapper() {}

  /** Map a Databricks/Delta SQL type string to a java.sql.Types code. */
  static int dbxToJdbc(String dbxTypeRaw) {
    if (dbxTypeRaw == null || dbxTypeRaw.isBlank()) return Types.VARCHAR;
    String t = dbxTypeRaw.trim().toUpperCase(Locale.ROOT);
    // strip parameters e.g. DECIMAL(18,2)
    int p = t.indexOf('(');
    if (p > 0) t = t.substring(0, p);


    switch (t) {
      case "STRING": case "CHAR": case "VARCHAR": return Types.VARCHAR;
      case "BINARY": return Types.BINARY;
      case "BOOLEAN": return Types.BOOLEAN;
      case "TINYINT": return Types.TINYINT;
      case "SMALLINT": return Types.SMALLINT;
      case "INT": case "INTEGER": return Types.INTEGER;
      case "BIGINT": return Types.BIGINT;
      case "FLOAT": return Types.FLOAT;
      case "DOUBLE": return Types.DOUBLE;
      case "DECIMAL": case "NUMERIC": return Types.DECIMAL;
      case "DATE": return Types.DATE;
      case "TIMESTAMP": case "TIMESTAMP_NTZ": return Types.TIMESTAMP;
      case "ARRAY": case "MAP": case "STRUCT": return Types.VARCHAR; // store as JSON text
      default: return Types.VARCHAR; // safe fallback
    }
  }
}
