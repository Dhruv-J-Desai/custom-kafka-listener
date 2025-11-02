package com.example.enterprise_kafka_starter.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configures our default ConsumerFactory and ListenerContainerFactory
 * used by @EnterpriseKafkaListener. Loads automatically via
 * META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
 */
@AutoConfiguration
@ConditionalOnClass(ConcurrentKafkaListenerContainerFactory.class)
@EnableConfigurationProperties(EnterpriseKafkaProperties.class)
public class EnterpriseKafkaAutoConfiguration {

    @Bean(name = "enterpriseConsumerFactory")
    @ConditionalOnMissingBean(name = "enterpriseConsumerFactory")
    ConsumerFactory<Object, Object> enterpriseConsumerFactory(
            KafkaProperties bootProps,
            EnterpriseKafkaProperties enterpriseProps) {

        Map<String, Object> props = new HashMap<>(bootProps.buildConsumerProperties(null));
        props.putAll(enterpriseProps.asKafkaOverrides());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "enterpriseKafkaListenerContainerFactory")
    @ConditionalOnMissingBean(name = "enterpriseKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<Object, Object> enterpriseKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> enterpriseConsumerFactory,
            EnterpriseKafkaProperties enterpriseProps) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(enterpriseConsumerFactory);

        // Enterprise defaults (tweakable via EnterpriseKafkaProperties)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(enterpriseProps.getPollTimeoutMs());
        factory.setCommonErrorHandler(enterpriseProps.buildErrorHandler());

        return factory;
    }
}
