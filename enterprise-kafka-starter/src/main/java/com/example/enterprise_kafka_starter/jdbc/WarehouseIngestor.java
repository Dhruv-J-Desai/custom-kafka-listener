package com.example.enterprise_kafka_starter.jdbc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class WarehouseIngestor {
    private static final Logger log = LoggerFactory.getLogger(WarehouseIngestor.class);
    private static final ObjectMapper M = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public enum Mode {INSERT, MERGE}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TradeEvent {
        public String tradeId;
        public String symbol;
        public Integer quantity;
        public Double price;
        public Instant valid_from; // optional
    }

    private final DataSource ds;
    private final String fqTable;
    private final Mode mode;
    private final int maxBatch;
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(50_000);
    private final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();

    public WarehouseIngestor(DataSource ds, String catalog, String schema, String table,
                             Mode mode, int maxBatch, long flushMs) {
        this.ds = ds;
        this.fqTable = String.format("%s.%s.%s", catalog, schema, table);
        this.mode = mode;
        this.maxBatch = maxBatch;
        sched.scheduleAtFixedRate(this::flushSafe, flushMs, flushMs, TimeUnit.MILLISECONDS);
        log.info("Databricks JDBC target={} mode={}", fqTable, mode);
    }

    public void enqueueJson(String json) {
        if (!queue.offer(json)) log.warn("Ingest queue full; dropping");
        if (queue.size() >= maxBatch) flushSafe();
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

        List<TradeEvent> items = new ArrayList<>(raw.size());
        for (String s : raw) items.add(M.readValue(s, TradeEvent.class));

        if (mode == Mode.INSERT) insertBatch(items);
        else mergeBatch(items);
    }

    private void insertBatch(List<TradeEvent> items) throws Exception {
        String sql = "INSERT INTO " + fqTable +
                " (trade_id, symbol, quantity, price, valid_from, tx_start) " +
                " VALUES (?, ?, ?, ?, ?, current_timestamp())";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (TradeEvent t : items) {
                ps.setString(1, t.tradeId);
                ps.setString(2, t.symbol);
                if (t.quantity == null) ps.setNull(3, Types.INTEGER);
                else ps.setInt(3, t.quantity);
                if (t.price == null) ps.setNull(4, Types.DOUBLE);
                else ps.setDouble(4, t.price);
                if (t.valid_from == null) ps.setNull(5, Types.TIMESTAMP);
                else ps.setTimestamp(5, Timestamp.from(t.valid_from));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void mergeBatch(List<TradeEvent> items) throws Exception {
        String staging = fqTable.replace('.', '_') + "_tmp_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE " + staging + " (" +
                    "trade_id STRING, symbol STRING, quantity INT, price DOUBLE, valid_from TIMESTAMP, tx_start TIMESTAMP)");
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO " + staging + " (trade_id, symbol, quantity, price, valid_from, tx_start) " +
                            "VALUES (?, ?, ?, ?, ?, current_timestamp())")) {
                for (TradeEvent t : items) {
                    ps.setString(1, t.tradeId);
                    ps.setString(2, t.symbol);
                    if (t.quantity == null) ps.setNull(3, Types.INTEGER);
                    else ps.setInt(3, t.quantity);
                    if (t.price == null) ps.setNull(4, Types.DOUBLE);
                    else ps.setDouble(4, t.price);
                    if (t.valid_from == null) ps.setNull(5, Types.TIMESTAMP);
                    else ps.setTimestamp(5, Timestamp.from(t.valid_from));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            st.execute("MERGE INTO " + fqTable + " t USING " + staging + " s ON t.trade_id = s.trade_id " +
                    "WHEN MATCHED THEN UPDATE SET symbol=s.symbol, quantity=s.quantity, price=s.price, " +
                    "valid_from=s.valid_from, tx_start=s.tx_start " +
                    "WHEN NOT MATCHED THEN INSERT *");
        } finally {
            try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + staging);
            } catch (Exception ignore) {
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        try { flush(); } catch (Exception e) { log.warn("final flush failed", e); }
        sched.shutdown();
    }
}
