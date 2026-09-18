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
public class LoadGenValidationService {

    private final CqlSession session;
    private final DemoProperties props;
    private final LoadGenConsumerService consumerService;

    public Map<String, Object> validate() {
        Map<String, Map<String, Object>> dbRows = fetchDbRows();
        Map<String, Map<String, Object>> hashmapRows = consumerService.snapshot();

        List<String> missingInHashmap = new ArrayList<>();
        List<Map<String, Object>> mismatched = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> entry : dbRows.entrySet()) {
            Map<String, Object> hashmapRow = hashmapRows.get(entry.getKey());
            if (hashmapRow == null) {
                missingInHashmap.add(entry.getKey());
            } else if (!rowsEqual(entry.getValue(), hashmapRow)) {
                Map<String, Object> diff = new LinkedHashMap<>();
                diff.put("pk", entry.getKey());
                diff.put("db", entry.getValue());
                diff.put("hashmap", hashmapRow);
                mismatched.add(diff);
            }
        }

        List<String> extraInHashmap = new ArrayList<>();
        for (String pk : hashmapRows.keySet()) {
            if (!dbRows.containsKey(pk)) {
                extraInHashmap.add(pk);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dbRowCount", dbRows.size());
        result.put("hashmapRowCount", hashmapRows.size());
        result.put("missingInHashmap", missingInHashmap);
        result.put("extraInHashmap", extraInHashmap);
        result.put("mismatchedRows", mismatched);
        result.put("identical", missingInHashmap.isEmpty() && extraInHashmap.isEmpty() && mismatched.isEmpty());
        return result;
    }

    private Map<String, Map<String, Object>> fetchDbRows() {
        String qualifiedTable = props.getLoadgen().getKeyspace() + "." + props.getLoadgen().getTable();
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

    private boolean rowsEqual(Map<String, Object> dbRow, Map<String, Object> hashmapRow) {
        for (Map.Entry<String, Object> entry : dbRow.entrySet()) {
            if (!hashmapRow.containsKey(entry.getKey())) {
                continue;
            }
            if (!Objects.equals(entry.getValue(), hashmapRow.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
