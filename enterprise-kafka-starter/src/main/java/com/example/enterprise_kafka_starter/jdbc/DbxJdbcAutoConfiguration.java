package com.example.enterprise_kafka_starter.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

@AutoConfiguration
@EnableConfigurationProperties(DatabricksProperties.class)
public class DbxJdbcAutoConfiguration {
    private HikariDataSource ds;

    @Bean(name = "databricksSqlDs")
    @ConditionalOnMissingBean(name = "databricksSqlDs")
    public DataSource databricksSqlDs(DatabricksProperties p) {
        String url = String.format(
                "jdbc:databricks://%s:443/default;" +
                        "HttpPath=%s;" +
                        "AuthMech=3;SSL=1;transportMode=http;" +
                        "EnableArrow=0;IgnoreTransactions=1;UseNativeQuery=1",
                p.getHost(), p.getHttpPath());

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername("token");
        cfg.setPassword(p.getToken());
        cfg.setMaximumPoolSize(8);
        cfg.setMinimumIdle(1);
        cfg.setPoolName("dbx-sql");
        ds = new HikariDataSource(cfg);
        return ds;
    }

    @PreDestroy
    public void close() {
        if (ds != null) ds.close();
    }
}
