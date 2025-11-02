package com.example.enterprise_kafka_starter.delta;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = com.example.enterprise_kafka_starter.spark.SparkAutoConfiguration.class)
@ConditionalOnClass(SparkSession.class)
@ConditionalOnBean(SparkSession.class)
public class DeltaAutoConfiguration {

    private static final String DEFAULT_DELTA_PATH = "abfss://bronze@storageaccountdemoapp.dfs.core.windows.net/trades/trade_events";

    @Bean
    @ConditionalOnMissingBean(DeltaSink.class)
    public DeltaSink deltaSink(SparkSession spark) {
        return new DeltaSink(spark, DEFAULT_DELTA_PATH);
    }
}
