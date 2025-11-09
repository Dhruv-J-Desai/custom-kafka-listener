package com.example.enterprise_kafka_starter.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class BitemporalIngestionService {

    private final DatabricksProperties props;
    private final TableInspector inspector;
    private final BronzeWriter bronzeWriter;
    private final SchemaValidator validator;
    private final Scd2Merger scd2;
    private final MappingEngine mapper = new MappingEngine();

    public void process(ConsumerRecord<String, String> rec) {
        String topic = rec.topic();
        FeedConfig feed = props.getFeeds().stream()
                .filter(f -> topic.equalsIgnoreCase(f.getTopic()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No feed mapped to topic: " + topic));

        String bronzeTable = feed.getBronze() != null ? feed.getBronze().getTable() : null;
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime eventTs = null;

        // try parse only event_ts for bronze metadata
        try {
            Object doc = mapper.parse(rec.value());
            if (feed.getMapping().containsKey(feed.getEventTimeColumn())) {
                Map<String, Object> tmp = mapper.apply(doc, Map.of(feed.getEventTimeColumn(), feed.getMapping().get(feed.getEventTimeColumn())));
                Object ts = tmp.get(feed.getEventTimeColumn());
                if (ts instanceof LocalDateTime) eventTs = (LocalDateTime) ts;
            }
        } catch (Exception ignore) {
        }

        // Bronze always
        try {
            if (bronzeTable != null) {
                if (props.getWorkspace().isCreateBronzeIfMissing() && !inspector.tableExists(bronzeTable)) {
                    inspector.createBronzeIfMissing(bronzeTable);
                }
                bronzeWriter.insertRaw(
                        bronzeTable,
                        rec.value(),
                        rec.topic(),
                        rec.key(),
                        rec.headers(),
                        now,
                        eventTs,
                        feed.getName()
                );
            }
        } catch (Exception e) {
            log.error("Bronze insert failed (feed={}): {}", feed.getName(), e.getMessage(), e);
        }

        // Silver only if both exist
        boolean silverExists, bronzeExists;
        try {
            silverExists = inspector.tableExists(feed.getTable());
            bronzeExists = bronzeTable == null || inspector.tableExists(bronzeTable);
        } catch (Exception e) {
            log.error("Table existence check failed: {}", e.getMessage(), e);
            return;
        }

        if (silverExists && bronzeExists) {
            try {
                Object doc = mapper.parse(rec.value());
                Map<String, Object> mapped = mapper.apply(doc, feed.getMapping());
                NormalizedRow row = NormalizedRow.builder()
                        .values(mapped)
                        .keyColumns(feed.getSchema().getKeyColumns())
                        .eventTimeColumn(feed.getEventTimeColumn())
                        .build();

                validator.validateOrThrow(feed);
                scd2.mergeRow(feed, row, true);

            } catch (Exception e) {
                log.error("Silver SCD2 failed (feed={}): {}", feed.getName(), e.getMessage(), e);
            }
        } else {
            log.info("Skipping Silver for feed={} (silverExists={}, bronzeExists={})", feed.getName(), silverExists, bronzeExists);
        }
    }
}
