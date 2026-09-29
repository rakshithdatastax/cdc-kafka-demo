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

@Service
@RequiredArgsConstructor
public class SchemaEvolutionService {

    private final CqlSession session;
    private final DemoProperties props;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, Object> current() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cassandra", cassandraSchema());
        result.put("kafka", kafkaSchemaHistory());
        return result;
    }

    private Map<String, Object> cassandraSchema() {
        var tableMeta = session.getMetadata()
                .getKeyspace(props.getCassandra().getKeyspace())
                .flatMap(ks -> ks.getTable(props.getCassandra().getTable()));

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
        result.put("keyspace", props.getCassandra().getKeyspace());
        result.put("table", props.getCassandra().getTable());
        result.put("columns", columns);
        result.put("columnCount", columns.size());
        return result;
    }

    private Map<String, Object> kafkaSchemaHistory() throws Exception {
        String subject = props.getKafka().getDataTopic() + "-value";
        String baseUrl = props.getKafka().getSchemaRegistryUrl();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("subject", subject);

        HttpResponse<String> versionsResponse = http.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (versionsResponse.statusCode() != 200) {
            result.put("versions", List.of());
            result.put("error", "subject not found yet -- has the connector published a row?");
            return result;
        }

        List<Integer> versionNumbers = new ArrayList<>();
        mapper.readTree(versionsResponse.body()).forEach(n -> versionNumbers.add(n.asInt()));
        versionNumbers.sort(Comparator.naturalOrder());

        List<Map<String, Object>> versions = new ArrayList<>();
        for (int version : versionNumbers) {
            HttpResponse<String> versionResponse = http.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions/" + version)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
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
            versions.add(versionEntry);
        }

        result.put("versions", versions);
        result.put("versionCount", versions.size());
        return result;
    }
}
