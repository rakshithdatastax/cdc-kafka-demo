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

    @NestedConfigurationProperty
    private Loadgen loadgen = new Loadgen();

    @NestedConfigurationProperty
    private Backfill backfill = new Backfill();

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

    // Separate keyspace/table/topic from Cassandra/Kafka above: the loadgen pod writes to its
    // own dedicated table (see k8s/loadgen.yaml), not the demo's own ks1.table1.
    @Data
    public static class Loadgen {
        private String keyspace;
        private String table;
        private String dataTopic;
    }
    @Data
    public static class Backfill {
        private String keyspace;
        private String table;
        private String dataTopic;
    }
}
