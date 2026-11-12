package com.example.loadgen;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Measures CDC latency. Key format: <prefix>-pk-<seq>-<t0EpochMs>.
 *   cassandra_to_event = event-topic ts - t0
 *   event_to_data      = data-topic ts  - event-topic ts
 *   end_to_end         = data-topic ts  - t0
 */
public class CdcLatencyMain {

    private static final Logger log = LoggerFactory.getLogger(CdcLatencyMain.class);

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        log.info("config: {}", config);

        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        HttpServer server = HttpServer.create(new InetSocketAddress(config.metricsPort), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();

        LatencyTracker tracker = new LatencyTracker(config, registry);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            tracker.stop();
            server.stop(0);
        }));
        tracker.start();
        tracker.join();
    }

    // ---------------------------------------------------------------- config

    public static class Config {
        String bootstrapServers;
        String schemaRegistryUrl;
        String eventTopic;
        String dataTopic;
        String keyPrefix;
        boolean startFromEnd;
        int metricsPort;

        static Config fromEnv() {
            Config c = new Config();
            c.bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
            c.schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://localhost:8080/apis/ccompat/v7");
            c.eventTopic = env("EVENT_TOPIC", "events-ks1.eventthroughput");
            c.dataTopic = env("DATA_TOPIC", "data-ks1.eventthroughput");
            c.keyPrefix = env("KEY_PREFIX", "");
            c.startFromEnd = Boolean.parseBoolean(env("START_FROM_END", "true"));
            c.metricsPort = Integer.parseInt(env("METRICS_PORT", "9404"));
            return c;
        }

        private static String env(String name, String def) {
            String v = System.getenv(name);
            return (v == null || v.isBlank()) ? def : v;
        }

        @Override
        public String toString() {
            return "bootstrap=" + bootstrapServers + " registry=" + schemaRegistryUrl
                    + " eventTopic=" + eventTopic + " dataTopic=" + dataTopic
                    + " keyPrefix=" + keyPrefix + " startFromEnd=" + startFromEnd
                    + " metricsPort=" + metricsPort;
        }
    }

    // --------------------------------------------------------------- tracker

    public static class LatencyTracker {

        /** Timestamps seen for one key; 0 = not seen yet. Guarded by synchronized(this). */
        private static final class Seen {
            long eventTs;
            long dataTs;
        }

        private static final long MIN_VALID_T0_MS = 1_000_000_000_000L;
        private static final Duration[] SLO = {
                Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
                Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofMillis(2500),
                Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(60)
        };

        private final Config config;
        private final MeterRegistry registry;
        private final Timer cassToEvent;
        private final Timer eventToData;
        private final Timer endToEnd;

        // Remembers the first timestamps per key until both topics have reported it.
        private final Cache<String, Seen> seen = Caffeine.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .maximumSize(5_000_000L)
                .build();

        private volatile boolean running = true;
        private Thread eventThread;
        private Thread dataThread;

        LatencyTracker(Config config, MeterRegistry registry) {
            this.config = config;
            this.registry = registry;
            this.cassToEvent = timer("cassandra_to_event");
            this.eventToData = timer("event_to_data");
            this.endToEnd = timer("end_to_end");
        }

        void start() {
            eventThread = startThread("event", config.eventTopic, this::onEvent);
            dataThread = startThread("data", config.dataTopic, this::onData);
        }

        void join() throws InterruptedException {
            eventThread.join();
            dataThread.join();
        }

        void stop() {
            running = false;
            eventThread.interrupt();
            dataThread.interrupt();
        }

        // ---- matching ----

        private void onEvent(String key, long ts) {
            Long t0 = parseT0(key);
            if (t0 == null) return;
            Seen s = seen.get(key, k -> new Seen());
            synchronized (s) {
                if (s.eventTs != 0) return; // replicas can emit several events per write: keep the first
                s.eventTs = ts;
                record(cassToEvent, "cassandra_to_event", ts - t0);
                if (s.dataTs != 0) recordDataSide(t0, s.eventTs, s.dataTs);
            }
        }

        private void onData(String key, long ts) {
            Long t0 = parseT0(key);
            if (t0 == null) return;
            Seen s = seen.get(key, k -> new Seen());
            synchronized (s) {
                if (s.dataTs != 0) return; // duplicate delivery
                s.dataTs = ts;
                if (s.eventTs != 0) recordDataSide(t0, s.eventTs, ts);
            }
        }

        private void recordDataSide(long t0, long eventTs, long dataTs) {
            record(eventToData, "event_to_data", dataTs - eventTs);
            record(endToEnd, "end_to_end", dataTs - t0);
        }

        private void record(Timer timer, String stage, long deltaMs) {
            if (deltaMs < 0) {
                // negative = clock skew between hosts; not recorded, but counted
                registry.counter("cdc.latency.negative", "stage", stage).increment();
            } else {
                timer.record(deltaMs, TimeUnit.MILLISECONDS);
            }
        }

        private Long parseT0(String key) {
            if (config.keyPrefix != null && !config.keyPrefix.isBlank()) {
                if (!key.startsWith(config.keyPrefix + "-pk-")) return null;
            } else {
                if (!key.contains("-pk-")) return null;
            }
            try {
                long t0 = Long.parseLong(key.substring(key.lastIndexOf('-') + 1));
                return t0 >= MIN_VALID_T0_MS ? t0 : null;
            } catch (NumberFormatException e) {
                return null; // e.g. initial-load keys without a t0 suffix
            }
        }

        private Timer timer(String stage) {
            return Timer.builder("cdc.latency").tag("stage", stage)
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(SLO)
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofMinutes(2))
                    .register(registry);
        }

        // ---- kafka ----

        private Thread startThread(String name, String topic, BiConsumer<String, Long> handler) {
            Thread t = new Thread(() -> consumeLoop(name, topic, handler), "cdc-latency-" + name);
            t.start();
            return t;
        }

        private void consumeLoop(String name, String topic, BiConsumer<String, Long> handler) {
            boolean isData = name.equals("data");

            Properties cp = new Properties();
            cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-latency-" + name + "-" + System.nanoTime());
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            cp.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false"); // don't create topics with default config

            KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer();
            keyDeserializer.configure(Map.of(
                    KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl,
                    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false), true);

            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cp)) {
                var initial = consumer.partitionsFor(topic);
                boolean existedAtStart = initial != null && !initial.isEmpty();

                List<TopicPartition> partitions = waitForPartitions(consumer, topic);
                if (partitions == null) return;
                consumer.assign(partitions);
                // If the topic appeared after we started, its first records are already in it: read from the start.
                if (config.startFromEnd && existedAtStart) {
                    consumer.seekToEnd(partitions);
                } else {
                    consumer.seekToBeginning(partitions);
                }
                log.info("{} consumer started on {} ({} partitions)", name, topic, partitions.size());

                boolean loggedTsType = false;
                while (running) {
                    for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                        if (!loggedTsType) {
                            log.info("{} topic timestamp type: {}", name, r.timestampType());
                            loggedTsType = true;
                        }
                        if (isData && r.value() == null) continue; // tombstone
                        try {
                            String key = AvroKeyDecoder.decodeId(r.key(), keyDeserializer, topic);
                            if (key != null) handler.accept(key, r.timestamp());
                        } catch (Exception e) {
                            log.warn("unreadable {} record at offset {}", name, r.offset(), e);
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    log.error("{} consumer died, exiting so the pod restarts", name, e);
                    System.exit(1);
                }
            }
        }

        private List<TopicPartition> waitForPartitions(KafkaConsumer<byte[], byte[]> consumer, String topic)
                throws InterruptedException {
            while (running) {
                var infos = consumer.partitionsFor(topic);
                if (infos != null && !infos.isEmpty()) {
                    return infos.stream().map(p -> new TopicPartition(topic, p.partition()))
                            .collect(Collectors.toList());
                }
                Thread.sleep(2000);
            }
            return null;
        }
    }

    // ------------------------------------------------------------ key decoder

    public static final class AvroKeyDecoder {
        private AvroKeyDecoder() {}

        public static String decodeId(byte[] bytes, KafkaAvroDeserializer keyDeserializer, String topic)
                throws Exception {
            if (bytes == null || bytes.length == 0) return null;
            if (bytes[0] == 0x0) { // Confluent wire format
                Object decoded = keyDeserializer.deserialize(topic, bytes);
                if (decoded instanceof GenericRecord record) {
                    Object id = record.get("id");
                    return id == null ? null : id.toString();
                }
                return decoded == null ? null : decoded.toString();
            }
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return new GenericDatumReader<Object>(Schema.create(Schema.Type.STRING)).read(null, decoder).toString();
        }
    }
}