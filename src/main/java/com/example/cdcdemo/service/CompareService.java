package com.example.cdcdemo.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.cdcdemo.config.DemoProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class CompareService {

    private final CqlSession session;
    private final DemoProperties props;
    private final OpenSearchService openSearchService;

    public Map<String, Object> compareOne(String id) throws Exception {
        Map<String, Object> cassandraRow = fetchDbRow(id);
        Map<String, Object> openSearchDoc = openSearchService.getDocument(id);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("cassandra", cassandraRow);
        result.put("openSearch", openSearchDoc);
        result.put("identical", cassandraRow != null && openSearchDoc != null && rowsEqual(cassandraRow, openSearchDoc));
        return result;
    }

    public Map<String, Object> compareAll() throws Exception {
        Map<String, Map<String, Object>> dbRows = fetchAllDbRows();
        Map<String, Map<String, Object>> osRows = openSearchService.getAllDocuments();

        List<String> missingInOpenSearch = new ArrayList<>();
        List<Map<String, Object>> mismatched = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> entry : dbRows.entrySet()) {
            Map<String, Object> osRow = osRows.get(entry.getKey());
            if (osRow == null) {
                missingInOpenSearch.add(entry.getKey());
            } else if (!rowsEqual(entry.getValue(), osRow)) {
                Map<String, Object> diff = new LinkedHashMap<>();
                diff.put("pk", entry.getKey());
                diff.put("cassandra", entry.getValue());
                diff.put("openSearch", osRow);
                mismatched.add(diff);
            }
        }

        List<String> extraInOpenSearch = new ArrayList<>();
        for (String pk : osRows.keySet()) {
            if (!dbRows.containsKey(pk)) {
                extraInOpenSearch.add(pk);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cassandraRowCount", dbRows.size());
        result.put("openSearchDocCount", osRows.size());
        result.put("missingInOpenSearch", missingInOpenSearch);
        result.put("extraInOpenSearch", extraInOpenSearch);
        result.put("mismatchedRows", mismatched);
        result.put("identical", missingInOpenSearch.isEmpty() && extraInOpenSearch.isEmpty() && mismatched.isEmpty());
        return result;
    }

    private Map<String, Object> fetchDbRow(String id) {
        String qualifiedTable = props.getCassandra().getKeyspace() + "." + props.getCassandra().getTable();
        Row row = session.execute("SELECT * FROM " + qualifiedTable + " WHERE id = ?", id).one();
        if (row == null) {
            return null;
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        row.getColumnDefinitions().forEach(def -> {
            String name = def.getName().asInternal();
            if (!name.equals("id")) {
                Object value = row.getObject(def.getName());
                fields.put(name, value == null ? null : value.toString());
            }
        });
        return fields;
    }

    private Map<String, Map<String, Object>> fetchAllDbRows() {
        String qualifiedTable = props.getCassandra().getKeyspace() + "." + props.getCassandra().getTable();
        Map<String, Map<String, Object>> rows = new LinkedHashMap<>();
        for (Row row : session.execute("SELECT * FROM " + qualifiedTable)) {
            String pk = row.getString("id");
            Map<String, Object> fields = new LinkedHashMap<>();
            row.getColumnDefinitions().forEach(def -> {
                String name = def.getName().asInternal();
                if (!name.equals("id")) {
                    Object value = row.getObject(def.getName());
                    fields.put(name, value == null ? null : value.toString());
                }
            });
            rows.put(pk, fields);
        }
        return rows;
    }

    private boolean rowsEqual(Map<String, Object> dbRow, Map<String, Object> otherRow) {
        for (Map.Entry<String, Object> entry : dbRow.entrySet()) {
            if (!otherRow.containsKey(entry.getKey())) {
                continue;
            }
            if (!Objects.equals(entry.getValue(), otherRow.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
