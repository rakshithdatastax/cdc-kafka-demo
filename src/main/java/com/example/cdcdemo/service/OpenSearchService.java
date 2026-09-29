package com.example.cdcdemo.service;

import com.example.cdcdemo.config.DemoProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class OpenSearchService {

    private final DemoProperties props;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public Map<String, Object> getDocument(String id) throws Exception {
        String url = props.getOpenSearch().getUrl() + "/" + props.getOpenSearch().getIndex() + "/_doc/" + id;
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 404) {
            return null;
        }
        JsonNode root = mapper.readTree(response.body());
        return fieldsToMap(root.path("_source"));
    }

    public Map<String, Map<String, Object>> getAllDocuments() throws Exception {
        String url = props.getOpenSearch().getUrl() + "/" + props.getOpenSearch().getIndex()
                + "/_search?size=10000";
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"query\":{\"match_all\":{}}}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        Map<String, Map<String, Object>> results = new LinkedHashMap<>();
        if (response.statusCode() != 200) {
            return results;
        }
        JsonNode hits = mapper.readTree(response.body()).path("hits").path("hits");
        for (JsonNode hit : hits) {
            String id = hit.path("_id").asText();
            results.put(id, fieldsToMap(hit.path("_source")));
        }
        return results;
    }

    public Map<String, Object> indexStats() throws Exception {
        String url = props.getOpenSearch().getUrl() + "/" + props.getOpenSearch().getIndex() + "/_count";
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("index", props.getOpenSearch().getIndex());
        if (response.statusCode() == 200) {
            result.put("docCount", mapper.readTree(response.body()).path("count").asInt());
        } else {
            result.put("docCount", 0);
            result.put("error", response.body());
        }
        return result;
    }

    private Map<String, Object> fieldsToMap(JsonNode source) {
        Map<String, Object> map = new LinkedHashMap<>();
        Iterator<String> names = source.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode value = source.get(name);
            map.put(name, value.isNull() ? null : value.asText());
        }
        return map;
    }
}
