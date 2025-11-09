package com.example.enterprise_kafka_starter.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

@Component
@RequiredArgsConstructor
class TableInspector {
    private final DataSource databricksSqlDs;

    boolean tableExists(String fullName) throws SQLException {
        String sql = "SHOW TABLE EXTENDED LIKE '" + fullName + "'";
        try (Connection c = databricksSqlDs.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        } catch (SQLException e) {
            try (Connection c = databricksSqlDs.getConnection();
                 Statement st = c.createStatement()) {
                st.executeQuery("DESCRIBE TABLE " + fullName);
                return true;
            } catch (SQLException ex) {
                return false;
            }
        }
    }

    Set<String> describeColumns(String table) throws SQLException {
        String sql = "DESCRIBE TABLE " + table;
        try (Connection c = databricksSqlDs.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            Set<String> cols = new HashSet<>();
            while (rs.next()) {
                String col = rs.getString(1);
                if (col == null || col.startsWith("#")) continue;
                cols.add(col);
            }
            return cols;
        }
    }

    void createBronzeIfMissing(String table) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " (\n" +
                "  payload STRING,\n" +
                "  topic STRING,\n" +
                "  key STRING,\n" +
                "  headers STRING,\n" +
                "  ingest_ts TIMESTAMP,\n" +
                "  event_ts TIMESTAMP,\n" +
                "  source_system STRING\n" +
                ") USING DELTA";
        try (Connection c = databricksSqlDs.getConnection();
             Statement st = c.createStatement()) {
            st.execute(ddl);
        }
    }
}
