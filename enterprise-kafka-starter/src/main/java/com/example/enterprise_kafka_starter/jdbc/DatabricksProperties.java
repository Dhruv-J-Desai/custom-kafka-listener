package com.example.enterprise_kafka_starter.jdbc;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

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

    public @NotBlank String getHost() {
        return host;
    }

    public void setHost(@NotBlank String host) {
        this.host = host;
    }

    public @NotBlank String getHttpPath() {
        return httpPath;
    }

    public void setHttpPath(@NotBlank String httpPath) {
        this.httpPath = httpPath;
    }

    public @NotBlank String getToken() {
        return token;
    }

    public void setToken(@NotBlank String token) {
        this.token = token;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public int getBatchMaxSize() {
        return batchMaxSize;
    }

    public void setBatchMaxSize(int batchMaxSize) {
        this.batchMaxSize = batchMaxSize;
    }

    public long getBatchFlushMs() {
        return batchFlushMs;
    }

    public void setBatchFlushMs(long batchFlushMs) {
        this.batchFlushMs = batchFlushMs;
    }
}
