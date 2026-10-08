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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Polls the Kafka Connect REST API directly for the health of every connector this demo cares
 * about -- the same /connectors/{name}/status calls used by hand all session, just surfaced in
 * the UI instead of a terminal.
 */
@Service
@RequiredArgsConstructor
public class ConnectHealthService {

    private final DemoProperties props;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    public List<Map<String, Object>> health() {
        List<Map<String, Object>> results = new ArrayList<>();
        String baseUrl = props.getKafkaConnect().getUrl();
        for (String name : props.getKafkaConnect().getConnectors()) {
            results.add(fetchOne(baseUrl, name));
        }
        return results;
    }

    private Map<String, Object> fetchOne(String baseUrl, String name) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("name", name);
        try {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + "/connectors/" + name + "/status"))
                            .timeout(java.time.Duration.ofSeconds(4))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                entry.put("reachable", true);
                entry.put("found", false);
                return entry;
            }
            if (response.statusCode() != 200) {
                entry.put("reachable", true);
                entry.put("found", false);
                entry.put("error", "HTTP " + response.statusCode());
                return entry;
            }
            JsonNode root = mapper.readTree(response.body());
            entry.put("reachable", true);
            entry.put("found", true);
            entry.put("type", root.path("type").asText(null));
            entry.put("connectorState", root.path("connector").path("state").asText(null));

            List<Map<String, Object>> tasks = new ArrayList<>();
            for (JsonNode taskNode : root.path("tasks")) {
                Map<String, Object> task = new LinkedHashMap<>();
                task.put("id", taskNode.path("id").asInt());
                task.put("state", taskNode.path("state").asText(null));
                String trace = taskNode.path("trace").asText(null);
                if (trace != null) {
                    task.put("trace", trace.length() > 400 ? trace.substring(0, 400) + "..." : trace);
                }
                tasks.add(task);
            }
            entry.put("tasks", tasks);
        } catch (Exception e) {
            entry.put("reachable", false);
            entry.put("error", e.getMessage());
        }
        return entry;
    }
}
