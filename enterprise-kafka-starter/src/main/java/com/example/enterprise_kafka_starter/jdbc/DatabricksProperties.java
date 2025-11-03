package com.example.enterprise_kafka_starter.jdbc;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "enterprise.databricks")
public class DatabricksProperties {
    private String host;
    private String httpPath;
    private String token;

    private String catalog;
    private String schema;
    private String table;

    private String mode;
    private int batchMaxSize;
    private long batchFlushMs;

    private List<String> mergeKeys = List.of();
    private List<String> updateAllowlist = List.of();
}
