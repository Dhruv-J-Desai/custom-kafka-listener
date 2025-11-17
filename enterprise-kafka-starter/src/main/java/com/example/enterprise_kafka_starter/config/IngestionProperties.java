package com.example.enterprise_kafka_starter.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "enterprise.kafka.ingestion")
public class IngestionProperties {

    /**
     * ingestion.mode = "auto" or "manual"
     * auto  -> SDK runs BitemporalIngestionService.process(...)
     * manual -> user must call it manually
     */
    private String mode = "auto"; // default auto-ingestion
}
