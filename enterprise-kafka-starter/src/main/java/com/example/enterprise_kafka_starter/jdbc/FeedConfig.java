package com.example.enterprise_kafka_starter.jdbc;

import lombok.Data;

import java.util.List;
import java.util.Map;

// FeedConfig.java
@Data
public class FeedConfig {
    private String name;
    private String topic;

    private Bronze bronze;            // <—— NEW
    private String table;             // silver target

    private String mode;
    private String eventTimeColumn;

    private Schema schema;
    private Map<String, String> mapping;

    private BitemporalColumns bitemporal;

    @Data
    public static class Bronze {
        private String table;         // e.g., main.bronze.trade_events_raw
    }

    @Data
    public static class Schema {
        private List<String> keyColumns;
        private List<SchemaAttribute> attributes;
    }
}
