package com.example.enterprise_kafka_starter.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;

final class JdbcBinds {
  private JdbcBinds(){}

  static void bind(PreparedStatement ps, int idx, Object v, int jdbcType) throws SQLException {
    if (v == null) {
      ps.setNull(idx, jdbcType == 0 ? Types.VARCHAR : jdbcType);
      return;
    }

    switch (jdbcType) {
      case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
        if (v instanceof Timestamp ts) {
          ps.setTimestamp(idx, ts);
        } else if (v instanceof LocalDateTime ldt) {
          ps.setTimestamp(idx, Timestamp.valueOf(ldt));
        } else {
          // last-resort parse
          ps.setTimestamp(idx, Timestamp.valueOf(v.toString().replace("Z","").replace('T',' ').substring(0,19)));
        }
      }
      case Types.DECIMAL, Types.NUMERIC -> {
        // Two safe options. Pick ONE.

        // OPTION A (simple & robust): bind as double (engine coerces to DECIMAL)
        double d = (v instanceof BigDecimal bd) ? bd.doubleValue()
                  : (v instanceof Number n)     ? n.doubleValue()
                  : Double.parseDouble(v.toString());
        ps.setDouble(idx, d);

        // OPTION B (exact scale/precision): uncomment if you prefer BigDecimal
        // ps.setBigDecimal(idx, (v instanceof BigDecimal bd) ? bd : new BigDecimal(v.toString()));
      }
      case Types.DOUBLE, Types.FLOAT -> {
        double d = (v instanceof Number n) ? n.doubleValue() : Double.parseDouble(v.toString());
        ps.setDouble(idx, d);
      }
      case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> {
        int i = (v instanceof Number n) ? n.intValue() : Integer.parseInt(v.toString());
        ps.setInt(idx, i);
      }
      case Types.BIGINT -> {
        long l = (v instanceof Number n) ? n.longValue() : Long.parseLong(v.toString());
        ps.setLong(idx, l);
      }
      case Types.BOOLEAN -> {
        boolean b = (v instanceof Boolean bo) ? bo : Boolean.parseBoolean(v.toString());
        ps.setBoolean(idx, b);
      }
      case Types.DATE -> {
        if (v instanceof java.sql.Date d) ps.setDate(idx, d);
        else ps.setDate(idx, java.sql.Date.valueOf(v.toString()));
      }
      default -> ps.setString(idx, v.toString());
    }
  }
}
