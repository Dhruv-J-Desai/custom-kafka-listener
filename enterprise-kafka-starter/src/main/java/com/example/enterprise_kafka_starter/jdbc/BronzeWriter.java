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
                   String sourceSystem,
                   String ingestionId,
                   String silverStatus,
                   String silverError,
                   LocalDateTime silverProcessedAt) throws Exception {

        String sql = "INSERT INTO " + bronzeTable +
                " (payload, topic, key, headers, ingest_ts, event_ts, source_system, ingestionId, silver_status, silver_error, silver_processed_at) " +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
            ps.setString(8, ingestionId);
            ps.setString(9, silverStatus);
            ps.setString(10, silverError);
            ps.setTimestamp(11, silverProcessedAt != null ? Timestamp.valueOf(silverProcessedAt) : null);
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

    public void markSilverStatus(
            String table,
            String ingestionId,
            String status,
            String error,
            LocalDateTime processedAt
    ) throws Exception {

        try (Connection conn = databricksSqlDs.getConnection()) {
            String sql = "UPDATE " + table +
                    " SET silver_status = ?, silver_error = ?, silver_processed_at = ?" +
                    " WHERE ingestion_id = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, status);
                ps.setString(2, error);
                ps.setTimestamp(3, processedAt != null ? Timestamp.valueOf(processedAt) : null);
                ps.setString(4, ingestionId);

                ps.executeUpdate();
            }
        }
    }
}
