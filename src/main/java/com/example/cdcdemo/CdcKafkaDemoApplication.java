package com.example.cdcdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class CdcKafkaDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(CdcKafkaDemoApplication.class, args);
    }
}
