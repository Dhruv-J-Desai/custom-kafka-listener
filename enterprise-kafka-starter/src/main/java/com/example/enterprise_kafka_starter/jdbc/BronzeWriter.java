package com.example.enterprise_kafka_starter.jdbc;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Base64;

@Component
@RequiredArgsConstructor
class BronzeWriter {
    private final DataSource databricksSqlDs;

    void insertRaw(String bronzeTable,
                   String payload,
                   String topic,
                   String key,
                   Headers headers,
                   LocalDateTime ingestTs,
                   LocalDateTime eventTs,
                   String sourceSystem) throws Exception {

        String sql = "INSERT INTO " + bronzeTable +
                " (payload, topic, key, headers, ingest_ts, event_ts, source_system) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection c = databricksSqlDs.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, payload);
            ps.setString(2, topic);
            ps.setString(3, key);
            ps.setString(4, headersToString(headers));

            ps.setTimestamp(5, Timestamp.valueOf(ingestTs));

            if (eventTs != null) ps.setTimestamp(6, Timestamp.valueOf(eventTs));
            else ps.setNull(6, Types.TIMESTAMP);

            ps.setString(7, sourceSystem);
            ps.executeUpdate();
        }
    }

    private String headersToString(Headers headers) {
        if (headers == null) return null;
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Header h : headers) {
            if (!first) sb.append(';');
            first = false;
            sb.append(h.key()).append('=');
            if (h.value() != null) sb.append(Base64.getEncoder().encodeToString(h.value()));
        }
        return sb.toString();
    }
}
