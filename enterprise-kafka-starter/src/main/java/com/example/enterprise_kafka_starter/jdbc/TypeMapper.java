// src/main/java/com/example/enterprise_kafka_starter/jdbc/TypeMapper.java
package com.example.enterprise_kafka_starter.jdbc;

import java.sql.Types;
import java.util.Locale;

final class TypeMapper {
  private TypeMapper(){}

  static int dbxToJdbc(String dbxType) {
    String t = dbxType.toUpperCase(Locale.ROOT);
    // strip params e.g. DECIMAL(10,2)
    int paren = t.indexOf('(');
    if (paren > 0) t = t.substring(0, paren);

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
      case "DECIMAL": return Types.DECIMAL;
      case "DATE": return Types.DATE;
      case "TIMESTAMP": return Types.TIMESTAMP;
      case "TIMESTAMP_NTZ": return Types.TIMESTAMP; // good enough for binding
      case "MAP": case "ARRAY": case "STRUCT":
        // store as JSON text by default (your JsonProjector will provide String)
        return Types.VARCHAR;
      default:
        // fallback to VARCHAR; you can extend as you meet more types
        return Types.VARCHAR;
    }
  }
}
