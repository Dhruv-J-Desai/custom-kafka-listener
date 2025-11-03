package com.example.enterprise_kafka_starter.jdbc;

import java.util.stream.Collectors;

final class SqlBuilders {

    private SqlBuilders() {
    }

    static String insertSql(TableMeta tm) {
        String cols = tm.columns.stream().map(c -> c.name).collect(Collectors.joining(", "));
        String qs = tm.columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + tm.fq + " (" + cols + ") VALUES (" + qs + ")";
    }

    static String mergeSql(TableMeta tm, java.util.Set<String> updateAllowlist) {
        if (tm.mergeKeys.isEmpty()) {
            throw new IllegalArgumentException("MERGE requested but no mergeKeys configured");
        }
        String on = tm.mergeKeys.stream()
                .map(k -> "t." + k + " = s." + k)
                .collect(Collectors.joining(" AND "));

        // UPDATE set list
        java.util.Set<String> updatable = (updateAllowlist == null || updateAllowlist.isEmpty())
                ? tm.columns.stream().map(c -> c.name).collect(Collectors.toSet())
                : updateAllowlist;

        String setList = tm.columns.stream()
                .map(c -> c.name)
                .filter(n -> !tm.mergeKeys.contains(n))   // never update keys
                .filter(updatable::contains)
                .map(n -> "t." + n + "=s." + n)
                .collect(Collectors.joining(", "));

        String cols = tm.columns.stream().map(c -> c.name).collect(Collectors.joining(", "));

        return "MERGE INTO " + tm.fq + " t USING %STAGING% s ON " + on + " " +
                (setList.isEmpty() ? "" : "WHEN MATCHED THEN UPDATE SET " + setList + " ") +
                "WHEN NOT MATCHED THEN INSERT (" + cols + ") VALUES (" + cols + ")";
    }
}
