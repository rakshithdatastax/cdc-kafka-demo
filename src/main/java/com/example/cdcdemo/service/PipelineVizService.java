package com.example.cdcdemo.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.cdcdemo.config.DemoProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Backs the "enter dummy data -> Kafka -> OpenSearch" pipeline visualizer tab: inserts a single
 * row into ks1.loadgen with a fresh id, then lets the frontend poll status(id) to watch that same
 * row appear at each stage as the real CDC pipeline picks it up -- no simulation, every stage
 * reflects the actual system.
 */
@Service
@RequiredArgsConstructor
public class PipelineVizService {

    private final CqlSession session;
    private final DemoProperties props;
    private final PipelineVizConsumerService kafkaView;
    private final OpenSearchService openSearchService;
    private final Random random = ThreadLocalRandom.current();

    public Map<String, Object> insertDummy(Integer value) {
        String id = "viz-" + System.currentTimeMillis();
        int v = value != null ? value : random.nextInt(1000);
        session.execute(
                String.format("INSERT INTO %s (id, c0) VALUES (?, ?)", qualifiedTable()),
                id, v);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("value", v);
        result.put("insertedAt", Instant.now().toString());
        return result;
    }

    public Map<String, Object> status(String id) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("cassandra", fetchCassandraRow(id));
        result.put("kafka", kafkaView.getRow(id));
        result.put("openSearch", openSearchService.getDocument(id));
        return result;
    }

    private Map<String, Object> fetchCassandraRow(String id) {
        Row row = session.execute("SELECT * FROM " + qualifiedTable() + " WHERE id = ?", id).one();
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

    private String qualifiedTable() {
        return props.getLoadgen().getKeyspace() + "." + props.getLoadgen().getTable();
    }
}
