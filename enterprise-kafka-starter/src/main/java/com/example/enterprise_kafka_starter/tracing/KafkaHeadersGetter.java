package com.example.enterprise_kafka_starter.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapts Kafka Headers to OpenTelemetry's TextMapGetter so we can
 * extract W3C traceparent/tracestate from message headers.
 */
public class KafkaHeadersGetter implements TextMapGetter<Headers> {

    @Override
    public Iterable<String> keys(Headers headers) {
        List<String> keys = new ArrayList<>();
        if (headers != null) {
            headers.forEach(h -> keys.add(h.key()));
        }
        return keys;
    }

    @Override
    public String get(Headers headers, String key) {
        if (headers == null || key == null) {
            return null;
        }
        Header header = headers.lastHeader(key);
        if (header == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
