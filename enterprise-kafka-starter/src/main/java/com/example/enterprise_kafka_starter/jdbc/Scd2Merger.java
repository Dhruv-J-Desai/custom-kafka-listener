package com.example.enterprise_kafka_starter.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.*;

/** SCD2 MERGE using a single-row USING subquery with typed placeholders. */
@Component
@RequiredArgsConstructor
class Scd2Merger {
  private final DataSource databricksSqlDs;

  void mergeRow(FeedConfig feed, NormalizedRow row, boolean nullsEqual) throws Exception {
    var keys  = feed.getSchema().getKeyColumns();
    var attrs = feed.getSchema().getAttributes();
    var bt    = feed.getBitemporal();

    // figure out which non-key columns we track for change
    List<String> tracked = new ArrayList<>();
    for (SchemaAttribute a : attrs) {
      if (a.getTrackChanges() == null || a.getTrackChanges()) tracked.add(a.getName());
    }

    // Build ordered source column list: keys + attributes + event_ts
    List<String> srcCols = new ArrayList<>(keys);
    for (SchemaAttribute a : attrs) srcCols.add(a.getName());
    String eventTsCol = feed.getEventTimeColumn();
    srcCols.add(eventTsCol);

    // Map column -> Databricks/Delta SQL type string (e.g., STRING, DECIMAL(18,2), TIMESTAMP)
    Map<String,String> dbxTypeByCol = new LinkedHashMap<>();
    // Keys: if you don’t have explicit types for keys, treat them as STRING
    for (String k : keys) dbxTypeByCol.put(k, "STRING");
    // Attributes: use the declared type
    for (SchemaAttribute a : attrs) dbxTypeByCol.put(a.getName(), a.getType());
    // event_ts is TIMESTAMP
    dbxTypeByCol.put(eventTsCol, "TIMESTAMP");

    // Also build JDBC types for binding (so our binder knows how to set values)
    Map<String,Integer> jdbcTypeByCol = new LinkedHashMap<>();
    for (var e : dbxTypeByCol.entrySet()) {
      jdbcTypeByCol.put(e.getKey(), TypeMapper.dbxToJdbc(e.getValue()));
    }

    // USING subquery with explicit CASTs so the driver doesn’t infer weird types
    // e.g.  SELECT CAST(? AS STRING) AS ClientId, CAST(? AS DECIMAL(18,2)) AS Price, ...
    String usingSelect = String.join(", ",
            srcCols.stream()
                    .map(c -> "CAST(? AS " + dbxTypeByCol.get(c) + ") AS " + c)
                    .toList()
    );

    String t  = feed.getTable();     // silver target
    String s  = "s";
    String ta = "t";

    // ON keys + only current rows
    String onKeys = keys.stream().map(k -> ta+"."+k+" = "+s+"."+k).reduce((a,b)->a+" AND "+b).orElse("1=0");

    // Change detection across tracked columns
    String changed = tracked.isEmpty()
            ? "false"
            : tracked.stream()
            .map(c -> ta+"."+c+" IS DISTINCT FROM "+s+"."+c)
            .reduce((a,b)->a+" OR "+b).orElse("false");

    String validFrom  = bt.getValidFromColumn();
    String validTo    = bt.getValidToColumn();
    String isCurrent  = bt.getIsCurrentColumn();

    // insert columns/values (SCD2 open-ended range)
    String insertCols = String.join(", ", srcCols) + ", " + validFrom + ", " + validTo + ", " + isCurrent;
    String insertVals = String.join(", ", srcCols.stream().map(c -> s+"."+c).toList())
            + ", " + s+"."+eventTsCol + ", TIMESTAMP '9999-12-31 23:59:59', true";

    String sql = ""
            + "MERGE INTO " + t + " " + ta + " \n"
            + "USING (SELECT " + usingSelect + ") " + s + " \n"
            + "ON (" + onKeys + " AND " + ta + "." + isCurrent + " = true)\n"
            + "WHEN MATCHED AND (" + changed + ") THEN UPDATE SET \n"
            + "  " + ta + "." + validTo   + " = " + s + "." + eventTsCol + ", \n"
            + "  " + ta + "." + isCurrent + " = false \n"
            + "WHEN NOT MATCHED THEN INSERT (" + insertCols + ") \n"
            + "VALUES (" + insertVals + ")";

    try (Connection c = databricksSqlDs.getConnection();
         PreparedStatement ps = c.prepareStatement(sql)) {

      // Bind values in the same order as srcCols, using explicit JDBC types
      int i = 1;
      for (String col : srcCols) {
        Object v = row.get(col);
        int jdbcType = jdbcTypeByCol.getOrDefault(col, Types.VARCHAR);
        JdbcBinds.bind(ps, i++, v, jdbcType);
      }
      ps.executeUpdate();
    }
  }
}
