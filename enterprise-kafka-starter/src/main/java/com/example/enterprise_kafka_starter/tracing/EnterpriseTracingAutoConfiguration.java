package com.example.enterprise_kafka_starter.tracing;

import io.opentelemetry.api.trace.Tracer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configures Kafka tracing around @EnterpriseKafkaListener methods.
 * Requires OpenTelemetry on the classpath and enterprise.tracing.enabled=true.
 */
@AutoConfiguration
@ConditionalOnClass(Tracer.class)
@EnableConfigurationProperties(EnterpriseTracingProperties.class)
public class EnterpriseTracingAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = "enterprise.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
    public KafkaTracingAspect kafkaTracingAspect(EnterpriseTracingProperties props) {
        return new KafkaTracingAspect(props);
    }
}
