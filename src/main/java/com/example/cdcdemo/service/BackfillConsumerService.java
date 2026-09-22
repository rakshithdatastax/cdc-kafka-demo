package com.example.cdcdemo.service;

import com.example.cdcdemo.config.DemoProperties;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
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
public class BackfillConsumerService {

    private final DemoProperties props;
    private final Map<String, Map<String, Object>> hashMap = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private volatile long messagesConsumed;
    private volatile long lastMessageAt;
    private Thread consumerThread;

    public BackfillConsumerService(DemoProperties props) {
        this.props = props;
    }

    @PostConstruct
    void start() {
        consumerThread = new Thread(this::consumeLoop, "backfill-demo-consumer");
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

    public Map<String, Map<String, Object>> snapshot() {
        return new LinkedHashMap<>(hashMap);
    }

    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("hashmapSize", hashMap.size());
        status.put("messagesConsumed", messagesConsumed);
        status.put("lastMessageAt", lastMessageAt == 0 ? null : java.time.Instant.ofEpochMilli(lastMessageAt).toString());
        return status;
    }

    private void consumeLoop() {
        String topic = props.getBackfill().getDataTopic();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getKafka().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-kafka-demo-backfill-consumer-" + System.nanoTime());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        avroDeserializer.configure(Map.of(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getKafka().getSchemaRegistryUrl(),
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false
        ), false);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = waitForPartitions(consumer, topic);
            if (partitions == null) {
                return; // stop() was called while waiting
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            log.info("backfill-demo consumer started on {} ({} partitions)", topic, partitions.size());

            while (running) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String key = decodeAvroStringKey(record.key());
                    if (record.value() == null) {
                        hashMap.remove(key);
                    } else {
                        GenericRecord decoded = (GenericRecord) avroDeserializer.deserialize(topic, record.value());
                        hashMap.put(key, genericRecordToMap(decoded));
                    }
                    messagesConsumed++;
                    lastMessageAt = System.currentTimeMillis();
                }
            }
        } catch (Exception e) {
            if (running) {
                log.error("backfill-demo consumer stopped unexpectedly", e);
            }
        }
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

    private String decodeAvroStringKey(byte[] bytes) throws Exception {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericDatumReader<Object> reader = new GenericDatumReader<>(stringSchema);
        return reader.read(null, decoder).toString();
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
