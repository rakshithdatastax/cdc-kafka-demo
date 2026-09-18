package com.example.cdcdemo.service;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.cdcdemo.config.DemoProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class CassandraDemoService {

    private final CqlSession session;
    private final DemoProperties props;

    public void createKeyspaceAndTable() {
        String keyspace = props.getCassandra().getKeyspace();
        String table = props.getCassandra().getTable();
        session.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
                        + "{'class': 'NetworkTopologyStrategy', 'dc1': 3}",
                keyspace));
        session.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, name text, age int) WITH cdc = true",
                keyspace, table));
    }

    public void insertRow(String id, String name, int age) {
        String qualified = qualifiedTable();
        session.execute(
                String.format("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", qualified),
                id, name, age);
    }

    public void addEmailColumnIfAbsent() {
        addColumnIfAbsent("email");
    }

    public void updateEmail(String id, String email) {
        updateColumn(id, "email", email);
    }
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    public void addColumnIfAbsent(String columnName) {
        requireSafeIdentifier(columnName);
        boolean columnExists = session.getMetadata()
                .getKeyspace(props.getCassandra().getKeyspace())
                .flatMap(ks -> ks.getTable(props.getCassandra().getTable()))
                .map(table -> table.getColumn(CqlIdentifier.fromCql(columnName)).isPresent())
                .orElse(false);
        if (!columnExists) {
            session.execute(String.format(
                    "ALTER TABLE %s ADD %s text", qualifiedTable(), columnName));
        }
    }

    public void updateColumn(String id, String columnName, String value) {
        requireSafeIdentifier(columnName);
        session.execute(
                String.format("UPDATE %s SET %s = ? WHERE id = ?", qualifiedTable(), columnName),
                value, id);
    }

    private static void requireSafeIdentifier(String columnName) {
        if (!SAFE_IDENTIFIER.matcher(columnName).matches()) {
            throw new IllegalArgumentException("Invalid column name: " + columnName);
        }
    }

    public void truncateTable() {
        session.execute(String.format("TRUNCATE %s", qualifiedTable()));
    }

    public void deleteRow(String id) {
        session.execute(
                String.format("DELETE FROM %s WHERE id = ?", qualifiedTable()),
                id);
    }

    public List<Map<String, Object>> selectAll() {
        List<Map<String, Object>> results = new ArrayList<>();
        for (Row row : session.execute(String.format("SELECT * FROM %s", qualifiedTable()))) {
            Map<String, Object> rowMap = new LinkedHashMap<>();
            row.getColumnDefinitions().forEach(def ->
                    rowMap.put(def.getName().asInternal(), row.getObject(def.getName())));
            results.add(rowMap);
        }
        return results;
    }

    private String qualifiedTable() {
        return props.getCassandra().getKeyspace() + "." + props.getCassandra().getTable();
    }
}
