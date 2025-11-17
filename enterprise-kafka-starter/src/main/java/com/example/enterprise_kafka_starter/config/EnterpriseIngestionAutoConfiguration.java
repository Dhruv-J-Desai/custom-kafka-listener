package com.example.enterprise_kafka_starter.config;

import com.example.enterprise_kafka_starter.jdbc.BitemporalIngestionService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@AutoConfiguration
@EnableConfigurationProperties(IngestionProperties.class)
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class EnterpriseIngestionAutoConfiguration {

    @Bean
    @ConditionalOnBean(BitemporalIngestionService.class)
    @ConditionalOnProperty(
            prefix = "enterprise.kafka.ingestion",
            name = "mode",
            havingValue = "auto",
            matchIfMissing = true
    )
    public EnterpriseIngestionAspect enterpriseIngestionAspect(
            BitemporalIngestionService ingestion,
            IngestionProperties props
    ) {
        return new EnterpriseIngestionAspect(ingestion, props);
    }
}
