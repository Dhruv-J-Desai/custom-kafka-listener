package com.example.enterprise_kafka_starter.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SCD2 upsert into a Delta (Databricks SQL) table using two statements:
 *  1) CLOSE current row (if any tracked attribute changed)  -- scalar UPDATE (no FROM)
 *  2) INSERT a new current row if none is current after step 1
 *
 * Dependencies:
 *  - JdbcBinds.bind(PreparedStatement, int, Object, int)
 *  - TypeMapper.dbxToJdbc(String dbxType)  // maps "DECIMAL(18,2)" -> Types.DECIMAL, "TIMESTAMP" -> Types.TIMESTAMP, etc.
 */
@Component
@RequiredArgsConstructor
class Scd2Merger {

    private final DataSource databricksSqlDs;

    void mergeRow(FeedConfig feed, NormalizedRow row, boolean nullsEqual) throws Exception {
        var keys  = feed.getSchema().getKeyColumns();
        var attrs = feed.getSchema().getAttributes();
        var bt    = feed.getBitemporal();

        // Which attributes are tracked for change?
        List<String> tracked = new ArrayList<>();
        for (SchemaAttribute a : attrs) {
            if (a.getTrackChanges() == null || a.getTrackChanges()) tracked.add(a.getName());
        }

        // Ordered column list for the "source row": keys + attributes + event_ts
        String eventTsCol = feed.getEventTimeColumn();
        List<String> srcCols = new ArrayList<>(keys);
        for (SchemaAttribute a : attrs) srcCols.add(a.getName());
        srcCols.add(eventTsCol);

        // DBX & JDBC type maps for safe casts/binds
        Map<String, String> dbxTypeByCol = new LinkedHashMap<>();
        for (String k : keys) dbxTypeByCol.put(k, "STRING");           // default key type (override if you prefer)
        for (SchemaAttribute a : attrs) dbxTypeByCol.put(a.getName(), a.getType());
        dbxTypeByCol.put(eventTsCol, "TIMESTAMP");

        Map<String, Integer> jdbcTypeByCol = new LinkedHashMap<>();
        for (var e : dbxTypeByCol.entrySet()) {
            jdbcTypeByCol.put(e.getKey(), TypeMapper.dbxToJdbc(e.getValue()));
        }

        String t         = feed.getTable();                 // silver target
        String validFrom = bt.getValidFromColumn();
        String validTo   = bt.getValidToColumn();
        String isCurrent = bt.getIsCurrentColumn();

        // =========================
        // 1) CLOSE current row
        // =========================
        // UPDATE t
        //   SET valid_to = :event_ts, is_current = false
        // WHERE is_current = true
        //   AND keys match (via CASTed placeholders)
        //   AND any tracked attribute is DISTINCT FROM its CASTed placeholder
        String keysEqUpdate = String.join(" AND ",
                keys.stream()
                        .map(k -> "t." + k + " = CAST(? AS " + dbxTypeByCol.get(k) + ")")
                        .toList());

        String changedPred = tracked.isEmpty()
                ? "false"
                : String.join(" OR ",
                tracked.stream()
                        .map(c -> "t." + c + " IS DISTINCT FROM CAST(? AS " + dbxTypeByCol.get(c) + ")")
                        .toList());

        String sqlClose =
                "UPDATE " + t + " t \n" +
                        "SET " + validTo + " = CAST(? AS TIMESTAMP), " + isCurrent + " = false \n" +
                        "WHERE t." + isCurrent + " = true \n" +
                        (keys.isEmpty() ? "" : "  AND (" + keysEqUpdate + ") \n") +
                        (tracked.isEmpty() ? "" : "  AND (" + changedPred + ")");

        // =========================
        // 2) INSERT new current row
        // =========================
        // INSERT INTO t (keys, attrs, event_ts, valid_from, valid_to, is_current)
        // SELECT s..., s.event_ts, s.event_ts, '9999-12-31...', true
        // FROM (SELECT CAST(? AS ...) AS col, ...) s
        // WHERE NOT EXISTS (SELECT 1 FROM t WHERE is_current = true AND keys match)
        String usingSelectInsert = String.join(", ",
                srcCols.stream()
                        .map(c -> "CAST(? AS " + dbxTypeByCol.get(c) + ") AS " + c)
                        .toList());

        String keyEqForNotExists = String.join(" AND ",
                keys.stream().map(k -> "t." + k + " = s." + k).toList());

        String insertCols = String.join(", ", srcCols) + ", " + validFrom + ", " + validTo + ", " + isCurrent;

        // For SCD2, valid_from = event_ts of this record; valid_to = open-ended
        String insertVals = String.join(", ", srcCols.stream().map(c -> "s." + c).toList())
                + ", s." + eventTsCol
                + ", TIMESTAMP '9999-12-31 23:59:59', true";

        String sqlInsert =
                "INSERT INTO " + t + " (" + insertCols + ") \n" +
                        "SELECT " + insertVals + " \n" +
                        "FROM (SELECT " + usingSelectInsert + ") s \n" +
                        "WHERE NOT EXISTS ( \n" +
                        "  SELECT 1 FROM " + t + " t \n" +
                        "  WHERE t." + isCurrent + " = true AND (" + keyEqForNotExists + ") \n" +
                        ")";

        try (Connection c = databricksSqlDs.getConnection()) {
            c.setAutoCommit(false);
            try {
                // ---- (1) CLOSE current if changed ----
                try (PreparedStatement ps = c.prepareStatement(sqlClose)) {
                    int p = 1;

                    // (a) event_ts for SET valid_to
                    JdbcBinds.bind(ps, p++, row.get(eventTsCol), Types.TIMESTAMP);

                    // (b) keys in order
                    for (String k : keys) {
                        int jdbc = jdbcTypeByCol.getOrDefault(k, Types.VARCHAR);
                        JdbcBinds.bind(ps, p++, row.get(k), jdbc);
                    }

                    // (c) tracked attrs in order
                    for (String col : tracked) {
                        int jdbc = jdbcTypeByCol.getOrDefault(col, Types.VARCHAR);
                        JdbcBinds.bind(ps, p++, row.get(col), jdbc);
                    }

                    ps.executeUpdate();
                }

                // ---- (2) INSERT new current if none exists now ----
                try (PreparedStatement ps = c.prepareStatement(sqlInsert)) {
                    int p = 1;
                    for (String col : srcCols) {
                        Object v   = row.get(col);
                        int jdbc   = jdbcTypeByCol.getOrDefault(col, Types.VARCHAR);
                        JdbcBinds.bind(ps, p++, v, jdbc);
                    }
                    ps.executeUpdate();
                }

                c.commit();
            } catch (Exception ex) {
                c.rollback();
                throw ex;
            } finally {
                c.setAutoCommit(true);
            }
        }
    }
}
