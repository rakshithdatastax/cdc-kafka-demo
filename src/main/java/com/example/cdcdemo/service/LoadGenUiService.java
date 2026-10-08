package com.example.cdcdemo.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.example.cdcdemo.config.DemoProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
public class LoadGenUiService {

    private static final String[] SAMPLE_NAMES = {
            "Ada", "Grace", "Alan", "Linus", "Barbara", "Edsger", "Margaret", "Ken"
    };

    private final CqlSession session;
    private final DemoProperties props;
    private final Random random = ThreadLocalRandom.current();

    public Map<String, Object> seed(int count) {
        String qualified = qualifiedTable();
        long stamp = System.currentTimeMillis();
        List<String> insertedIds = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String id = "ui-" + stamp + "-" + i;
            String name = SAMPLE_NAMES[random.nextInt(SAMPLE_NAMES.length)];
            int age = 20 + random.nextInt(50);
            session.execute(
                    String.format("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", qualified),
                    id, name, age);
            insertedIds.add(id);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("inserted", insertedIds.size());
        result.put("ids", insertedIds);
        return result;
    }

    public Map<String, Object> scatterUpdate(int count) {
        List<String> ids = existingIds();
        int updated = 0;
        List<String> touched = new ArrayList<>();
        for (int i = 0; i < count && !ids.isEmpty(); i++) {
            String id = ids.get(random.nextInt(ids.size()));
            int newAge = 20 + random.nextInt(50);
            session.execute(
                    String.format("UPDATE %s SET age = ? WHERE id = ?", qualifiedTable()),
                    newAge, id);
            touched.add(id);
            updated++;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("updated", updated);
        result.put("ids", touched);
        return result;
    }

    public Map<String, Object> deleteRandom(int count) {
        List<String> ids = existingIds();
        int deleted = 0;
        List<String> removed = new ArrayList<>();
        for (int i = 0; i < count && !ids.isEmpty(); i++) {
            String id = ids.remove(random.nextInt(ids.size()));
            session.execute(String.format("DELETE FROM %s WHERE id = ?", qualifiedTable()), id);
            removed.add(id);
            deleted++;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deleted", deleted);
        result.put("ids", removed);
        return result;
    }

    public Map<String, Object> evolveSchema() {
        int nextN = discoverNextExtraColumnIndex();
        String newColumn = "extra_" + nextN;
        session.execute(String.format("ALTER TABLE %s ADD %s int", qualifiedTable(), newColumn));
        List<String> ids = existingIds();
        int sampleSize = Math.min(ids.size(), 5);
        for (int i = 0; i < sampleSize; i++) {
            String id = ids.get(random.nextInt(ids.size()));
            session.execute(
                    String.format("UPDATE %s SET %s = ? WHERE id = ?", qualifiedTable(), newColumn),
                    random.nextInt(1000), id);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("addedColumn", newColumn);
        result.put("populatedRows", sampleSize);
        return result;
    }

    private int discoverNextExtraColumnIndex() {
        var tableMeta = session.getMetadata()
                .getKeyspace(props.getCassandra().getKeyspace())
                .flatMap(ks -> ks.getTable(props.getCassandra().getTable()));
        if (tableMeta.isEmpty()) {
            return 1;
        }
        int maxN = 0;
        for (ColumnMetadata col : tableMeta.get().getColumns().values()) {
            String name = col.getName().asInternal();
            if (name.startsWith("extra_")) {
                try {
                    maxN = Math.max(maxN, Integer.parseInt(name.substring("extra_".length())));
                } catch (NumberFormatException ignored) {
                    // non-numeric suffix, not one of ours
                }
            }
        }
        return maxN + 1;
    }

    private List<String> existingIds() {
        List<String> ids = new ArrayList<>();
        for (Row row : session.execute("SELECT id FROM " + qualifiedTable())) {
            ids.add(row.getString("id"));
        }
        return ids;
    }

    private String qualifiedTable() {
        return props.getCassandra().getKeyspace() + "." + props.getCassandra().getTable();
    }
}
