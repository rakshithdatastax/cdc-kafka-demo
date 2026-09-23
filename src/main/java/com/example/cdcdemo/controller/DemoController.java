package com.example.cdcdemo.controller;

import com.example.cdcdemo.config.DemoProperties;
import com.example.cdcdemo.service.CassandraDemoService;
import com.example.cdcdemo.service.KafkaMessageService;
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
}
