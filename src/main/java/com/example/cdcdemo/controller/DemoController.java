package com.example.cdcdemo.controller;

import com.example.cdcdemo.config.DemoProperties;
import com.example.cdcdemo.service.CassandraDemoService;
import com.example.cdcdemo.service.CompareService;
import com.example.cdcdemo.service.ConnectHealthService;
import com.example.cdcdemo.service.KafkaMessageService;
import com.example.cdcdemo.service.LoadGenUiService;
import com.example.cdcdemo.service.PipelineVizService;
import com.example.cdcdemo.service.SchemaEvolutionService;
import com.example.cdcdemo.service.Table1ConsumerService;
import com.example.cdcdemo.service.Table1ValidationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class DemoController {

    private final CassandraDemoService cassandraDemoService;
    private final KafkaMessageService kafkaMessageService;
    private final Table1ConsumerService table1ConsumerService;
    private final Table1ValidationService table1ValidationService;
    private final LoadGenUiService loadGenUiService;
    private final SchemaEvolutionService schemaEvolutionService;
    private final CompareService compareService;
    private final PipelineVizService pipelineVizService;
    private final ConnectHealthService connectHealthService;
    private final DemoProperties props;

    @PostMapping("/create-table")
    public Map<String, String> createTable() {
        cassandraDemoService.createKeyspaceAndTable();
        return Map.of("status", "created keyspace "
                + props.getCassandra().getKeyspace() + " and table "
                + props.getCassandra().getTable() + " with cdc=true");
    }

    @PostMapping("/insert")
    public List<Map<String, Object>> insert(
            @RequestParam String id,
            @RequestParam String name,
            @RequestParam int age) {
        cassandraDemoService.insertRow(id, name, age);
        return cassandraDemoService.selectAll();
    }

    @PostMapping("/alter")
    public List<Map<String, Object>> alterAndUpdate(
            @RequestParam String id,
            @RequestParam String email) {
        cassandraDemoService.addEmailColumnIfAbsent();
        cassandraDemoService.updateEmail(id, email);
        return cassandraDemoService.selectAll();
    }

    @PostMapping("/alter-column")
    public List<Map<String, Object>> alterColumnAndUpdate(
            @RequestParam String id,
            @RequestParam String column,
            @RequestParam String value) {
        cassandraDemoService.addColumnIfAbsent(column);
        cassandraDemoService.updateColumn(id, column, value);
        return cassandraDemoService.selectAll();
    }

    @DeleteMapping("/delete/{id}")
    public List<Map<String, Object>> delete(@PathVariable String id) {
        cassandraDemoService.deleteRow(id);
        return cassandraDemoService.selectAll();
    }

    @DeleteMapping("/truncate")
    public List<Map<String, Object>> truncate() {
        cassandraDemoService.truncateTable();
        return cassandraDemoService.selectAll();
    }

    @GetMapping("/select")
    public List<Map<String, Object>> select() {
        return cassandraDemoService.selectAll();
    }

    @GetMapping("/kafka/events")
    public List<Map<String, Object>> readEventsTopic() {
        return kafkaMessageService.readAllMessages(props.getKafka().getEventsTopic(), false);
    }

    @GetMapping("/kafka/data")
    public List<Map<String, Object>> readDataTopic() {
        return kafkaMessageService.readAllMessages(props.getKafka().getDataTopic(), true);
    }

    @GetMapping("/table1/status")
    public Map<String, Object> table1Status() {
        return table1ConsumerService.status();
    }

    @GetMapping("/table1/validate")
    public Map<String, Object> table1Validate() {
        return table1ValidationService.validate();
    }

    // --- Loadgen-style dummy data generation, triggerable from the UI ---

    @PostMapping("/loadgen-ui/seed")
    public Map<String, Object> loadGenSeed(@RequestParam(defaultValue = "10") int count) {
        return loadGenUiService.seed(count);
    }

    @PostMapping("/loadgen-ui/update")
    public Map<String, Object> loadGenUpdate(@RequestParam(defaultValue = "5") int count) {
        return loadGenUiService.scatterUpdate(count);
    }

    @PostMapping("/loadgen-ui/delete")
    public Map<String, Object> loadGenDelete(@RequestParam(defaultValue = "1") int count) {
        return loadGenUiService.deleteRandom(count);
    }

    @PostMapping("/loadgen-ui/evolve-schema")
    public Map<String, Object> loadGenEvolveSchema() {
        return loadGenUiService.evolveSchema();
    }

    // --- Schema evolution, Cassandra vs. Kafka schema registry ---

    @GetMapping("/schema-evolution")
    public Map<String, Object> schemaEvolution() throws Exception {
        return schemaEvolutionService.current();
    }

    // --- Live Cassandra vs. OpenSearch comparison ---

    @GetMapping("/compare/{id}")
    public Map<String, Object> compareOne(@PathVariable String id) throws Exception {
        return compareService.compareOne(id);
    }

    @GetMapping("/compare")
    public Map<String, Object> compareAll() throws Exception {
        return compareService.compareAll();
    }

    // --- Pipeline visualizer: one dummy row, watched through Cassandra -> Kafka -> OpenSearch ---

    @PostMapping("/pipeline-viz/insert")
    public Map<String, Object> pipelineVizInsert(@RequestParam(required = false) Integer value) {
        return pipelineVizService.insertDummy(value);
    }

    @GetMapping("/pipeline-viz/status/{id}")
    public Map<String, Object> pipelineVizStatus(@PathVariable String id) throws Exception {
        return pipelineVizService.status(id);
    }

    // --- Kafka Connect connector health ---

    @GetMapping("/connectors/health")
    public List<Map<String, Object>> connectorsHealth() {
        return connectHealthService.health();
    }
}
