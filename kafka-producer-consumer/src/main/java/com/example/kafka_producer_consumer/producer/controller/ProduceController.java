package com.example.kafka_producer_consumer.producer.controller;

import com.example.kafka_producer_consumer.producer.model.TradeEvent;
import com.example.kafka_producer_consumer.producer.service.TradeEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/produce")
public class ProduceController {
    private final TradeEventProducer producer;

    public ProduceController(TradeEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> produce(@RequestBody Map<String, Object> body) {
        String tradeId = UUID.randomUUID().toString();
        String symbol = (String) body.getOrDefault("symbol", "AAPL");
        int quantity = ((Number) body.getOrDefault("quantity", 100)).intValue();
        double price = ((Number) body.getOrDefault("price", 199.99)).doubleValue();

        producer.send(new TradeEvent(tradeId, symbol, quantity, price));

        return ResponseEntity.ok(Map.of(
                "status", "SENT",
                "tradeId", tradeId,
                "symbol", symbol,
                "quantity", quantity,
                "price", price
        ));
    }
}
