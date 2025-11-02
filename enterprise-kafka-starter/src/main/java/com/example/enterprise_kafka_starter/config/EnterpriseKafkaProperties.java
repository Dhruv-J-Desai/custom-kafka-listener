package com.example.enterprise_kafka_starter.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Opinionated consumer knobs exposed under `enterprise.kafka.consumer.*`.
 * These layer on top of standard `spring.kafka.*` properties.
 */
@ConfigurationProperties(prefix = "enterprise.kafka.consumer")
public class EnterpriseKafkaProperties {

    /** At-least-once by default; let app code ack manually. */
    private boolean enableAutoCommit = false;

    /** earliest | latest */
    private String autoOffsetReset = "earliest";

    /** Max records per poll. */
    private int maxPollRecords = 500;

    /** Poll timeout (ms) used by container. */
    private long pollTimeoutMs = 1500;

    /** Error handler backoff settings (basic default). */
    private long backoffDelayMs = 1000;
    private int backoffMaxRetries = 3;

    public Map<String, Object> asKafkaOverrides() {
        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return map;
    }

    public DefaultErrorHandler buildErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(backoffDelayMs, backoffMaxRetries));
    }

    // getters/setters
    public boolean isEnableAutoCommit() { return enableAutoCommit; }
    public void setEnableAutoCommit(boolean enableAutoCommit) { this.enableAutoCommit = enableAutoCommit; }

    public String getAutoOffsetReset() { return autoOffsetReset; }
    public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }

    public int getMaxPollRecords() { return maxPollRecords; }
    public void setMaxPollRecords(int maxPollRecords) { this.maxPollRecords = maxPollRecords; }

    public long getPollTimeoutMs() { return pollTimeoutMs; }
    public void setPollTimeoutMs(long pollTimeoutMs) { this.pollTimeoutMs = pollTimeoutMs; }

    public long getBackoffDelayMs() { return backoffDelayMs; }
    public void setBackoffDelayMs(long backoffDelayMs) { this.backoffDelayMs = backoffDelayMs; }

    public int getBackoffMaxRetries() { return backoffMaxRetries; }
    public void setBackoffMaxRetries(int backoffMaxRetries) { this.backoffMaxRetries = backoffMaxRetries; }
}
