package com.example.kafka_producer_consumer.consumer;

import com.example.enterprise_kafka_starter.annotation.EnterpriseKafkaListener;
import com.example.enterprise_kafka_starter.delta.DeltaSink;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class EventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventsConsumer.class);
    private final DeltaSink deltaSink;

    public EventsConsumer(DeltaSink deltaSink) {
        this.deltaSink = deltaSink;
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

            deltaSink.enqueue(record.value());

            // Commit offset (manual)
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Processing failed, delegating to error handler (retries/DLQ if configured)", e);
            // Re-throw so the container's DefaultErrorHandler in your starter handles retry/backoff
            throw e;
        }
    }
}
