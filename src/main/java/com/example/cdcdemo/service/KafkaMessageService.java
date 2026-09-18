package com.example.cdcdemo.service;

import com.example.cdcdemo.config.DemoProperties;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class KafkaMessageService {

    private final DemoProperties props;

    /**
     * Reads every message currently on a topic, from the beginning, using a throwaway consumer
     * group each call. Values on the connector's output topic are Avro-encoded against the
     * schema registry; the raw CDC events topic uses the agent's own internal mutation format, so
     * only the output topic is decoded to a readable record -- the events topic is shown as a
     * byte count, useful just to confirm activity happened.
     */
    public List<Map<String, Object>> readAllMessages(String topic, boolean decodeAvro) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getKafka().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-kafka-demo-" + System.nanoTime());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaAvroDeserializer avroDeserializer = null;
        if (decodeAvro) {
            avroDeserializer = new KafkaAvroDeserializer();
            avroDeserializer.configure(Map.of(
                    KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getKafka().getSchemaRegistryUrl(),
                    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false
            ), false);
        }

        List<Map<String, Object>> results = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(PartitionInfo::partition)
                    .map(p -> new TopicPartition(topic, p))
                    .collect(Collectors.toList());
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty() && !results.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("partition", record.partition());
                    entry.put("offset", record.offset());
                    entry.put("key", record.key() == null ? null : new String(record.key()));
                    if (record.value() == null) {
                        entry.put("value", null);
                        entry.put("note", "tombstone (row deleted)");
                    } else if (decodeAvro) {
                        entry.put("value", avroDeserializer.deserialize(topic, record.value()).toString());
                    } else {
                        entry.put("valueBytes", record.value().length);
                    }
                    results.add(entry);
                }
            }
        }
        return results;
    }
}
