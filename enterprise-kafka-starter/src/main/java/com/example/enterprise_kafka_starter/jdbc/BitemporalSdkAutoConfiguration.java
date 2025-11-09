package com.example.enterprise_kafka_starter.jdbc;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

/**
 * Auto-config to register all SDK beans (service + helpers) from the starter.
 * This lets apps depend on the starter without manually component-scanning its package.
 */
@AutoConfiguration
@ComponentScan(basePackageClasses = { BitemporalIngestionService.class })
public class BitemporalSdkAutoConfiguration {
    // no @Bean methods needed — we just scan the package where the SDK beans live
}
