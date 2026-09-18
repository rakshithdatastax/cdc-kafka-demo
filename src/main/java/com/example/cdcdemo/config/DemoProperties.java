package com.example.cdcdemo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@Data
@ConfigurationProperties(prefix = "demo")
public class DemoProperties {

    @NestedConfigurationProperty
    private Cassandra cassandra = new Cassandra();

    @NestedConfigurationProperty
    private Kafka kafka = new Kafka();

    @Data
    public static class Cassandra {
        private String contactPoint;
        private int port;
        private String localDatacenter;
        private String keyspace;
        private String table;
        private String username;
        private String password;
    }

    @Data
    public static class Kafka {
        private String bootstrapServers;
        private String schemaRegistryUrl;
        private String eventsTopic;
        private String dataTopic;
    }
}
