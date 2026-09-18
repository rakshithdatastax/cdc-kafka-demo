package com.example.cdcdemo.controller;

import com.example.cdcdemo.service.LoadGenConsumerService;
import com.example.cdcdemo.service.LoadGenValidationService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/demo/loadgen")
@RequiredArgsConstructor
public class LoadGenController {

    private final LoadGenConsumerService consumerService;
    private final LoadGenValidationService validationService;

    @GetMapping("/status")
    public Map<String, Object> status() {
        return consumerService.status();
    }
    @GetMapping("/validate")
    public Map<String, Object> validate() {
        return validationService.validate();
    }
}
