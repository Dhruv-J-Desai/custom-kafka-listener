package com.example.enterprise_kafka_starter.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

final class TableMeta {
    final String catalog, schema, table, fq;
    final List<Column> columns;          // ordered
    final Set<String> mergeKeys;         // configured

    static final class Column {
        final String name;         // column_name in DBX
        final String dbxType;      // e.g. "STRING", "INT", "DOUBLE", "TIMESTAMP", "DECIMAL(10,2)"
        final int jdbcType;        // java.sql.Types.*

        Column(String name, String dbxType, int jdbcType) {
            this.name = name;
            this.dbxType = dbxType;
            this.jdbcType = jdbcType;
        }
    }

    TableMeta(String catalog, String schema, String table,
              List<Column> columns, Set<String> mergeKeys) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.fq = catalog + "." + schema + "." + table;
        this.columns = columns;
        this.mergeKeys = (mergeKeys == null ? Set.of() : Set.copyOf(mergeKeys));
    }

    static TableMeta load(Connection c, String catalog, String schema, String table, Set<String> mergeKeys) throws SQLException {
        String sql = """
                  SELECT column_name, UPPER(data_type)
                  FROM %s.information_schema.columns
                  WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
                  ORDER BY ordinal_position
                """.formatted(catalog);
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            List<Column> cols = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String dbxType = rs.getString(2);
                    int jdbc = TypeMapper.dbxToJdbc(dbxType);
                    cols.add(new Column(name, dbxType, jdbc));
                }
            }
            if (cols.isEmpty()) {
                throw new SQLException("No columns found for " + catalog + "." + schema + "." + table);
            }
            return new TableMeta(catalog, schema, table, cols, mergeKeys);
        }
    }
}
