package com.example.enterprise_kafka_starter.tracing;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tracing knobs for the enterprise starter.
 * Exporter / endpoint stays in the Java agent env vars;
 * here we only control whether SDK-level spans are created.
 */
@Data
@ConfigurationProperties(prefix = "enterprise.tracing")
public class EnterpriseTracingProperties {

    /**
     * Master switch. If false, the tracing aspect does nothing.
     */
    private boolean enabled = true;

    /**
     * Name used when getting a Tracer from GlobalOpenTelemetry.
     * This is not the service.name; that comes from the agent/env.
     */
    private String tracerName = "enterprise-kafka-consumer";

    /**
     * Whether to add basic messaging attributes (topic, partition, offset).
     */
    private boolean addMessagingAttributes = true;
}
