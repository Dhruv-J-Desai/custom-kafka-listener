package com.example.enterprise_kafka_starter.jdbc;

import com.databricks.internal.sdk.service.provisioning.Workspace;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "enterprise.databricks")
public class DatabricksProperties {
    private String host;
    private String httpPath;
    private String token;

/*    private Workspace workspace;*/
    private List<FeedConfig> feeds;

/*    @Data
    public static class Workspace {
        private String validationPolicy = "fail";
        private boolean createBronzeIfMissing = true;
        private String defaultTimezone = "UTC";
    }*/
}
