package com.example.enterprise_kafka_starter.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;

@Component
@RequiredArgsConstructor
class SchemaValidator {
    private final TableInspector inspector;

    void validateOrThrow(FeedConfig feed) throws Exception {
        var cols = inspector.describeColumns(feed.getTable());
        var required = new java.util.HashSet<String>(feed.getSchema().getKeyColumns());
        feed.getSchema().getAttributes().forEach(a -> required.add(a.getName()));
        required.add(feed.getEventTimeColumn());
        required.add(feed.getBitemporal().getValidFromColumn());
        required.add(feed.getBitemporal().getValidToColumn());
        required.add(feed.getBitemporal().getIsCurrentColumn());

        var missing = new LinkedHashSet<String>();
        for (String r : required) if (!cols.contains(r)) missing.add(r);

        if (!missing.isEmpty())
            throw new IllegalStateException("Silver table missing columns: " + missing);
    }
}
