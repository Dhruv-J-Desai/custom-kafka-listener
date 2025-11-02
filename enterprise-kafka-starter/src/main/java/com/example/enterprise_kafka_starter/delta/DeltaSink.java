package com.example.enterprise_kafka_starter.delta;

import jakarta.annotation.PreDestroy;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.spark.sql.functions.current_timestamp;

public class DeltaSink {
    private final SparkSession spark;
    private final String deltaPath;

    // backpressure-friendly buffer
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(50_000);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // tune these for throughput/latency
    private static final int MAX_BATCH = 1_000;   // flush when N messages buffered
    private static final int FLUSH_MS = 2_000;   // or every M milliseconds

    // explicit schema (match your payload fields)
    private static final StructType SCHEMA = new StructType()
            .add("tradeId", "string")
            .add("symbol", "string")
            .add("quantity", "int")
            .add("price", "double")
            .add("valid_from", "timestamp"); // include if you do bi-temporal

    public DeltaSink(SparkSession spark, String deltaPath) {
        this.spark = spark;
        this.deltaPath = deltaPath;
        // periodic flusher
        scheduler.scheduleAtFixedRate(this::flushSafe, FLUSH_MS, FLUSH_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Called by your Kafka listener
     */
    public void enqueue(String json) {
        // offer() avoids blocking your Kafka poll thread; add fallback if dropping must be avoided
        boolean ok = queue.offer(json);
        if (!ok) {
            // If backpressure is strict, block: queue.put(json);
            // Or send to a "write-failed" topic/metric.
            // For now we drop & rely on logging/metrics if you want.
        }
        if (queue.size() >= MAX_BATCH) flushSafe();
    }

    private void flushSafe() {
        try {
            flush();
        } catch (Exception e) { /* log e */ }
    }

    /**
     * One write per batch for efficiency
     */
    public synchronized void flush() {
        List<String> batch = new ArrayList<>(MAX_BATCH);
        queue.drainTo(batch, MAX_BATCH);
        if (batch.isEmpty()) return;

        Encoder<String> enc = Encoders.STRING();
        Dataset<Row> df = spark.read()
                .schema(SCHEMA)
                .json(spark.createDataset(batch, enc))
                .withColumn("tx_start", current_timestamp());  // system time

        // Optional: validate/clean here (null checks, ranges, cast failures)

        df.write()
                .format("delta")
                .mode("append")
                .save(deltaPath);
    }

    @PreDestroy
    public void shutdown() {
        // flush before shutdown
        flushSafe();
        scheduler.shutdown();
    }
}
