package com.example.enterprise_kafka_starter.jdbc;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class WarehouseIngestor {
    private static final Logger log = LoggerFactory.getLogger(WarehouseIngestor.class);

    public enum Mode {INSERT, MERGE}

    private final DataSource ds;
    private final Mode mode;
    private final int maxBatch;
    private final long flushMs;
    private final Set<String> mergeKeys;             // e.g., ["trade_id"]
    private final Set<String> updateAllowlist;       // optional

    private volatile TableMeta tableMeta;            // lazy-loaded & cached
    private volatile String insertSql;               // cached
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(50_000);
    private final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();

    private final String catalog, schema, table;

    public WarehouseIngestor(DataSource ds,
                             String catalog, String schema, String table,
                             Mode mode, int maxBatch, long flushMs,
                             Set<String> mergeKeys, Set<String> updateAllowlist) {
        this.ds = ds;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.mode = mode;
        this.maxBatch = maxBatch;
        this.flushMs = flushMs;
        this.mergeKeys = (mergeKeys == null ? Set.of() : Set.copyOf(mergeKeys));
        this.updateAllowlist = (updateAllowlist == null ? Set.of() : Set.copyOf(updateAllowlist));
        sched.scheduleAtFixedRate(this::flushSafe, flushMs, flushMs, TimeUnit.MILLISECONDS);
        log.info("Databricks JDBC target={}.{}.{} mode={} keys={}", catalog, schema, table, mode, this.mergeKeys);
    }

    public void enqueueJson(String json) {
        if (!queue.offer(json)) log.warn("Ingest queue full; dropping");
        if (queue.size() >= maxBatch) flushSafe();
    }

    private void ensureMeta(Connection c) throws Exception {
        if (tableMeta != null) return;
        TableMeta tm = TableMeta.load(c, catalog, schema, table, mergeKeys);
        this.tableMeta = tm;
        this.insertSql = SqlBuilders.insertSql(tm);
    }

    private void flushSafe() {
        try {
            flush();
        } catch (Exception e) {
            log.error("flush failed", e);
        }
    }

    synchronized void flush() throws Exception {
        List<String> raw = new ArrayList<>(maxBatch);
        queue.drainTo(raw, maxBatch);
        if (raw.isEmpty()) return;

        // parse once; keep only columns that exist in the table
        List<Map<String, Object>> rows = new ArrayList<>(raw.size());
        for (String js : raw) {
            Map<String, Object> m = JsonProjector.toRow(js);
            rows.add(m);
        }

        try (Connection c = ds.getConnection()) {
            ensureMeta(c);
            if (mode == Mode.INSERT) insertBatch(c, rows);
            else mergeBatch(c, rows);
        }
    }

    // somewhere in WarehouseIngestor (private method)
    private static void bind(PreparedStatement ps, int paramIndex, Object value, int jdbcType) throws Exception {
        if (value == null) {
            ps.setNull(paramIndex, jdbcType);
        } else {
            ps.setObject(paramIndex, value);
        }
    }

    private void insertBatch(Connection c, List<Map<String, Object>> rows) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(insertSql)) {
            for (Map<String, Object> row : rows) {
                Object[] params = JsonProjector.projectToColumns(row, tableMeta.columns);
                for (int i = 0; i < params.length; i++) {
                    int jdbcType = tableMeta.columns.get(i).jdbcType;
                    bind(ps, i + 1, params[i], jdbcType);
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void mergeBatch(Connection c, List<Map<String, Object>> rows) throws Exception {
        String staging = (catalog + "_" + schema + "_" + table + "_tmp_" + UUID.randomUUID().toString().replace("-", ""));
        String colList = tableMeta.columns.stream().map(col -> col.name).collect(Collectors.joining(", "));
        String insertStaging = "INSERT INTO " + staging + " (" + colList + ") VALUES (" +
                tableMeta.columns.stream().map(col -> "?").collect(Collectors.joining(", ")) + ")";

        try (Statement st = c.createStatement()) {
            // create staging with same columns (types can be relaxed to STRING if needed)
            String create = "CREATE TABLE " + staging + " AS SELECT * FROM " + tableMeta.fq + " WHERE 1=0";
            st.execute(create);
        }
        try (PreparedStatement ps = c.prepareStatement(insertStaging)) {
            for (Map<String, Object> row : rows) {
                Object[] params = JsonProjector.projectToColumns(row, tableMeta.columns);
                for (int i = 0; i < params.length; i++) {
                    int jdbcType = tableMeta.columns.get(i).jdbcType;
                    bind(ps, i + 1, params[i], jdbcType);      // <-- typed null-safe bind
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }

        String merge = SqlBuilders.mergeSql(tableMeta, updateAllowlist).replace("%STAGING%", staging);
        try (Statement st = c.createStatement()) {
            st.execute(merge);
        } finally {
            try (Statement st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + staging);
            } catch (Exception ignore) {
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            flush();
        } catch (Exception e) {
            log.warn("final flush failed", e);
        }
        sched.shutdown();
    }
}