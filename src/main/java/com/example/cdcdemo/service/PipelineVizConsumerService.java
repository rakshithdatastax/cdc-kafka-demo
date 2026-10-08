package com.example.cdcdemo.service;

import com.example.cdcdemo.config.DemoProperties;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PipelineVizConsumerService {

    private static final int MAX_TRACKED_ROWS = 500;

    private final DemoProperties props;
    private final Map<String, Map<String, Object>> recentRows = new ConcurrentHashMap<>();
    private final Map<String, Long> arrivalOrder = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private volatile long messagesConsumed;
    private Thread consumerThread;

    public PipelineVizConsumerService(DemoProperties props) {
        this.props = props;
    }

    @PostConstruct
    void start() {
        consumerThread = new Thread(this::consumeLoop, "pipeline-viz-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @PreDestroy
    void stop() {
        running = false;
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
    }

    public Map<String, Object> getRow(String id) {
        return recentRows.get(id);
    }

    private void consumeLoop() {
        String topic = props.getLoadgen().getDataTopic();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getKafka().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-kafka-demo-pipeline-viz-" + System.nanoTime());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaAvroDeserializer valueDeserializer = new KafkaAvroDeserializer();
        valueDeserializer.configure(Map.of(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getKafka().getSchemaRegistryUrl(),
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false
        ), false);
        KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer();
        keyDeserializer.configure(Map.of(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getKafka().getSchemaRegistryUrl(),
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false
        ), true);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = waitForPartitions(consumer, topic);
            if (partitions == null) {
                return; // stop() was called while waiting
            }
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            log.info("pipeline-viz consumer started on {} ({} partitions), tailing from latest", topic, partitions.size());

            while (running) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        String key = AvroKeyDecoder.decodeId(record.key(), keyDeserializer, topic);
                        if (key == null) {
                            continue;
                        }
                        if (record.value() == null) {
                            recentRows.remove(key);
                            arrivalOrder.remove(key);
                        } else {
                            GenericRecord decoded = (GenericRecord) valueDeserializer.deserialize(topic, record.value());
                            recentRows.put(key, genericRecordToMap(decoded));
                            arrivalOrder.put(key, System.nanoTime());
                            evictOldestIfNeeded();
                        }
                    } catch (Exception recordError) {
                        log.warn("skipping unreadable pipeline-viz record at offset {}", record.offset(), recordError);
                    }
                    messagesConsumed++;
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("pipeline-viz consumer stopped unexpectedly", e);
            }
        }
    }

    private void evictOldestIfNeeded() {
        if (recentRows.size() <= MAX_TRACKED_ROWS) {
            return;
        }
        arrivalOrder.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .ifPresent(oldest -> {
                    recentRows.remove(oldest.getKey());
                    arrivalOrder.remove(oldest.getKey());
                });
    }

    private List<TopicPartition> waitForPartitions(KafkaConsumer<byte[], byte[]> consumer, String topic) throws InterruptedException {
        while (running) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos != null && !partitionInfos.isEmpty()) {
                return partitionInfos.stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .collect(Collectors.toList());
            }
            Thread.sleep(2000);
        }
        return null;
    }

    private Map<String, Object> genericRecordToMap(GenericRecord record) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            map.put(field.name(), value == null ? null : value.toString());
        }
        return map;
    }
}
