package com.example.kafka_producer_consumer.producer.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestProducerController {
    private final KafkaTemplate<String, String> kafkaTemplate;

    private String topic = "trade-events";

    @PostMapping("/crm")
    public ResponseEntity<String> sendCrmEvent(@RequestBody String payload) {
        kafkaTemplate.send(topic, payload);
        return ResponseEntity.ok("Sent to topic " + topic);
    }
}
