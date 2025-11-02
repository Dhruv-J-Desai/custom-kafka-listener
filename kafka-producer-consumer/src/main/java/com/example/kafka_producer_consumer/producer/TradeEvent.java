package com.example.kafka_producer_consumer.producer;

public record TradeEvent(
        String tradeId,
        String symbol,
        int quantity,
        double price
) {
}
