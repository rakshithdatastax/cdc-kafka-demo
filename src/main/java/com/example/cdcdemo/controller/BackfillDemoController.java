package com.example.cdcdemo.controller;

import com.example.cdcdemo.service.BackfillConsumerService;
import com.example.cdcdemo.service.BackfillDemoService;
import com.example.cdcdemo.service.BackfillValidationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/demo/backfill")
@RequiredArgsConstructor
public class BackfillDemoController {

    private final BackfillDemoService backfillDemoService;
    private final BackfillConsumerService consumerService;
    private final BackfillValidationService validationService;

    @PostMapping("/create-table")
    public Map<String, String> createTable() {
        return Map.of("status", backfillDemoService.createTableCdcDisabled());
    }

    @PostMapping("/seed")
    public Map<String, String> seed(@RequestParam(defaultValue = "500") int count) {
        return Map.of("status", backfillDemoService.seed(count));
    }

    @PostMapping("/enable-cdc")
    public Map<String, String> enableCdc() {
        return Map.of("status", backfillDemoService.enableCdc());
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return consumerService.status();
    }

    @GetMapping("/validate")
    public Map<String, Object> validate() {
        return validationService.validate();
    }
}
