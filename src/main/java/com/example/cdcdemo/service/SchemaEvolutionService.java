package com.example.cdcdemo.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.example.cdcdemo.config.DemoProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class SchemaEvolutionService {

    private final CqlSession session;
    private final DemoProperties props;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, Object> current() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cassandra", allTablesCassandraSchema());
        result.put("kafka", allSubjectsSchemaHistory());
        return result;
    }

    // Both ks1.table1 (the demo table) and ks1.loadgen (what the loadgen pod actually writes to,
    // and the table with real schema-evolution history) feed Kafka subjects shown alongside this,
    // so both need to be listed here -- not just the demo table.
    private Map<String, Object> allTablesCassandraSchema() {
        List<Map<String, Object>> tables = new ArrayList<>();
        tables.add(tableSchema(props.getCassandra().getKeyspace(), props.getCassandra().getTable()));
        tables.add(tableSchema(props.getLoadgen().getKeyspace(), props.getLoadgen().getTable()));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tables", tables);
        return result;
    }

    private Map<String, Object> tableSchema(String keyspace, String table) {
        var tableMeta = session.getMetadata()
                .getKeyspace(keyspace)
                .flatMap(ks -> ks.getTable(table));

        List<Map<String, String>> columns = new ArrayList<>();
        if (tableMeta.isPresent()) {
            for (ColumnMetadata col : tableMeta.get().getColumns().values()) {
                Map<String, String> column = new LinkedHashMap<>();
                column.put("name", col.getName().asInternal());
                column.put("type", col.getType().toString());
                columns.add(column);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("keyspace", keyspace);
        result.put("table", table);
        result.put("columns", columns);
        result.put("columnCount", columns.size());
        return result;
    }

    // Fetches every subject's version history concurrently -- sequential round trips through the
    // schema-registry port-forward (one per subject, one per version) add up to several seconds
    // once there are a dozen-plus versions across subjects.
    private Map<String, Object> allSubjectsSchemaHistory() throws Exception {
        String baseUrl = props.getKafka().getSchemaRegistryUrl();

        Map<String, Object> result = new LinkedHashMap<>();

        HttpResponse<String> subjectsResponse = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/subjects")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (subjectsResponse.statusCode() != 200) {
            result.put("subjects", List.of());
            result.put("error", "could not reach schema registry at " + baseUrl);
            return result;
        }

        List<String> subjectNames = new ArrayList<>();
        mapper.readTree(subjectsResponse.body()).forEach(n -> subjectNames.add(n.asText()));
        subjectNames.sort(Comparator.naturalOrder());

        List<Map<String, Object>> subjects = subjectNames.stream()
                .map(subject -> subjectSchemaHistoryAsync(subject, baseUrl))
                .toList()
                .stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        result.put("subjects", subjects);
        result.put("subjectCount", subjects.size());
        return result;
    }

    private CompletableFuture<Map<String, Object>> subjectSchemaHistoryAsync(String subject, String baseUrl) {
        return http.sendAsync(
                        HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions")).GET().build(),
                        HttpResponse.BodyHandlers.ofString())
                .thenCompose(versionsResponse -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("subject", subject);

                    if (versionsResponse.statusCode() != 200) {
                        result.put("versions", List.of());
                        result.put("error", "subject not found yet -- has the connector published a row?");
                        return CompletableFuture.completedFuture(result);
                    }

                    List<Integer> versionNumbers = new ArrayList<>();
                    try {
                        mapper.readTree(versionsResponse.body()).forEach(n -> versionNumbers.add(n.asInt()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    List<CompletableFuture<Map<String, Object>>> versionFutures = versionNumbers.stream()
                            .map(version -> fetchVersionAsync(subject, version, baseUrl))
                            .toList();

                    return CompletableFuture.allOf(versionFutures.toArray(new CompletableFuture[0]))
                            .thenApply(ignored -> {
                                List<Map<String, Object>> versions = versionFutures.stream()
                                        .map(CompletableFuture::join)
                                        .sorted(Comparator.comparingInt(m -> (int) m.get("version")))
                                        .collect(Collectors.toList());
                                result.put("versions", versions);
                                result.put("versionCount", versions.size());
                                return result;
                            });
                });
    }

    private CompletableFuture<Map<String, Object>> fetchVersionAsync(String subject, int version, String baseUrl) {
        return http.sendAsync(
                        HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions/" + version)).GET().build(),
                        HttpResponse.BodyHandlers.ofString())
                .thenApply(versionResponse -> {
                    try {
                        JsonNode versionJson = mapper.readTree(versionResponse.body());
                        JsonNode schemaJson = mapper.readTree(versionJson.path("schema").asText());

                        List<String> fields = new ArrayList<>();
                        for (JsonNode field : schemaJson.path("fields")) {
                            fields.add(field.path("name").asText());
                        }

                        Map<String, Object> versionEntry = new LinkedHashMap<>();
                        versionEntry.put("version", version);
                        versionEntry.put("schemaId", versionJson.path("id").asInt());
                        versionEntry.put("fields", fields);
                        return versionEntry;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
