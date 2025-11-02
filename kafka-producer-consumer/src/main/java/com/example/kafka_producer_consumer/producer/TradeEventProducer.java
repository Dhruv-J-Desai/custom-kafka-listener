package com.example.kafka_producer_consumer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TradeEventProducer {
    private static final String TOPIC = "trade-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public TradeEventProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public void send(TradeEvent event) {
        String key = event.tradeId();
        String payload = toJson(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, payload);
        kafkaTemplate.send(record);
        System.out.println("Produced record to trade-events topic: " + record);
    }

    private String toJson(TradeEvent e) {
        try {
            return mapper.writeValueAsString(e);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Failed to serialize TradeEvent", ex);
        }
    }
}
