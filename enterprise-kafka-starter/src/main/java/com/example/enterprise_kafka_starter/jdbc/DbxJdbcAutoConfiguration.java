package com.example.enterprise_kafka_starter.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

@AutoConfiguration
@EnableConfigurationProperties(DatabricksProperties.class)
@ConditionalOnProperty(prefix = "enterprise.databricks", name = {"host","http-path","token"})
public class DbxJdbcAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(DbxJdbcAutoConfiguration.class);

    @Bean
    public DataSource databricksDataSource(DatabricksProperties p) {
        String url = String.format(
                "jdbc:databricks://%s:443/default;transportMode=http;ssl=1;AuthMech=3;UID=token;PWD=%s;httpPath=%s;EnableArrow=1;IgnoreTransactions=1;UseNativeQuery=1",
                p.getHost(), p.getToken(), p.getHttpPath());

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setDriverClassName("com.databricks.client.jdbc.Driver");
        cfg.setMaximumPoolSize(5);
        cfg.setMinimumIdle(1);
        log.info("Configured Databricks JDBC url for host={} (httpPath hidden)", p.getHost());
        return new HikariDataSource(cfg);
    }

    @Bean
    @ConditionalOnMissingBean(WarehouseIngestor.class)
    public WarehouseIngestor warehouseIngestor(DataSource ds, DatabricksProperties p) {
        return new WarehouseIngestor(
                ds,
                p.getCatalog(),
                p.getSchema(),
                p.getTable(),
                WarehouseIngestor.Mode.valueOf(p.getMode().toUpperCase()),
                p.getBatchMaxSize(),
                p.getBatchFlushMs());
    }
}
