package com.example.enterprise_kafka_starter.config;

import com.example.enterprise_kafka_starter.annotation.EnterpriseKafkaListener;
import com.example.enterprise_kafka_starter.jdbc.BitemporalIngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Arrays;

@Aspect
@Slf4j
@RequiredArgsConstructor
public class EnterpriseIngestionAspect {

    private final BitemporalIngestionService ingestion;
    private final IngestionProperties props;

    @Around("@annotation(listener)")
    public Object aroundEnterpriseListener(ProceedingJoinPoint pjp,
                                           EnterpriseKafkaListener listener) throws Throwable {

        boolean auto = props.getMode().equalsIgnoreCase("auto");

        Object[] args = pjp.getArgs();
        ConsumerRecord<String, String> record = findRecord(args);
        Acknowledgment ack = findAck(args);

        if (!auto) {
            // MANUAL MODE — do nothing extra
            return pjp.proceed();   // app handles ingestion & ack
        }

        // AUTO MODE
        try {
            // 1. plugin hook (optional business transformation)
            Object result = pjp.proceed();

            ConsumerRecord<String, String> recordToIngest = record;
            if (result instanceof ConsumerRecord<?, ?> r) {
                @SuppressWarnings("unchecked")
                ConsumerRecord<String, String> cast = (ConsumerRecord<String, String>) r;
                recordToIngest = cast;
            }

            // 2. SDK ingestion
            ingestion.process(recordToIngest);

            log.info("Written to Databricks table");

            // 3. SDK ack
            if (ack != null) ack.acknowledge();

            return null;

        } catch (Exception e) {
            log.error("Auto ingestion failed", e);
            throw e; // propagate to DLQ handler
        }
    }

    private ConsumerRecord<String, String> findRecord(Object[] args) {
        return (ConsumerRecord<String, String>) Arrays.stream(args)
                .filter(a -> a instanceof ConsumerRecord<?, ?>)
                .findFirst()
                .orElse(null);
    }

    private Acknowledgment findAck(Object[] args) {
        return (Acknowledgment) Arrays.stream(args)
                .filter(a -> a instanceof Acknowledgment)
                .findFirst()
                .orElse(null);
    }
}
