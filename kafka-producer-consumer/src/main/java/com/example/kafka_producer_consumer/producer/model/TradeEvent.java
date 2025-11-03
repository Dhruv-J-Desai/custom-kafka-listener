package com.example.kafka_producer_consumer.producer.model;

public record TradeEvent(
        String tradeId,
        String symbol,
        int quantity,
        double price
) {
}
