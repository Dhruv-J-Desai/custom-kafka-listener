package com.example.kafka_producer_consumer.consumer;

import com.example.enterprise_kafka_starter.annotation.EnterpriseKafkaListener;
import com.example.enterprise_kafka_starter.jdbc.BitemporalIngestionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventsConsumer {

    private final BitemporalIngestionService ingestion;

    public EventsConsumer(BitemporalIngestionService ingestion) {
        this.ingestion = ingestion;
    }

    @EnterpriseKafkaListener(
            topics = "trade-events",
            groupId = "tcen-consumers",
            concurrency = "2"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            log.info("Consume: partition={}, offset={}, key={}, payload={}",
                    record.partition(), record.offset(), record.key(), record.value());
            ingestion.process(record);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Processing failed, delegating to error handler", e);
            throw e;
        }
    }
}
