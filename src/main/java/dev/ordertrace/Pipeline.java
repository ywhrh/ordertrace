package dev.ordertrace;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/** Kafka EOS ends at the result topic. The separate database writer is at-least-once. */
@Component
@ConditionalOnProperty(name="ordertrace.pipeline-enabled", havingValue="true", matchIfMissing=true)
public class Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
    private final Settings settings;
    private final ReadModel model;
    private KafkaStreams streams;
    private Thread writer;
    private volatile boolean running;
    private volatile String writerState = "STARTING";
    public Pipeline(Settings settings, ReadModel model) { this.settings = settings; this.model = model; }

    public static void topics(Settings settings) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", settings.bootstrap()))) {
            for (String name : List.of(settings.rawTopic(), settings.resultTopic())) {
                try { admin.createTopics(List.of(new NewTopic(name, 3, (short)1)
                        .configs(Map.of("retention.ms", "604800000", "cleanup.policy", "delete"))))
                        .all().get(30, java.util.concurrent.TimeUnit.SECONDS); }
                catch (ExecutionException e) { if (!(e.getCause() instanceof TopicExistsException)) throw e; }
            }
        }
    }
    public static Properties streamsProperties(Settings settings) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrap());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, settings.applicationId());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, settings.stateDir());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return properties;
    }
    @PostConstruct public void start() throws Exception {
        writerState = "STARTING";
        topics(settings);
        streams = new KafkaStreams(LifecycleTopology.build(settings.rawTopic(), settings.resultTopic()), streamsProperties(settings));
        streams.setUncaughtExceptionHandler(exception -> {
            LOG.error("Stream stopped; invalid/conflicting evidence must be investigated", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        running = true;
        writer = Thread.ofPlatform().name("postgres-writer").start(this::consume);
        streams.start();
    }
    private void consume() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrap());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.writerGroup());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(settings.resultTopic()));
            while (running) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(500));
                boolean failed = false;
                for (ConsumerRecord<String,String> record : records) {
                    // Finish this batch on shutdown; committing a partially processed poll would skip evidence.
                    try {
                        Aggregate.Snapshot snapshot = Json.read(record.value(), Aggregate.Snapshot.class);
                        if (!snapshot.rootOrderId().equals(record.key())) throw new IllegalArgumentException("Result key mismatch");
                        model.persist(snapshot); // Returns only after PostgreSQL transaction commits.
                        consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)));
                        writerState = "RUNNING";
                    } catch (Exception exception) {
                        writerState = "RETRYING";
                        LOG.warn("Writer will retry uncommitted results: {}", exception.toString());
                        // Rewind ALL polled partitions: poll already advanced their positions, even for unprocessed records.
                        for (TopicPartition partition : records.partitions())
                            consumer.seek(partition, records.records(partition).getFirst().offset());
                        failed = true;
                        break;
                    }
                }
                if (failed) Thread.sleep(1000);
                else {
                    // Also commit positions past Kafka transaction markers, needed for reliable replay lag checks.
                    try { consumer.commitSync(); } catch (org.apache.kafka.common.KafkaException e) { LOG.warn("Offset commit will retry: {}", e.toString()); }
                    if (records.isEmpty() && !writerState.equals("RETRYING")) writerState = "RUNNING";
                }
            }
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        catch (Exception exception) { if (running) { writerState = "FAILED"; LOG.error("Database writer stopped", exception); } }
    }
    public Map<String,String> status() { return Map.of("streams", streams == null ? "STARTING" : streams.state().name(), "writer", writerState); }
    @PreDestroy public void stop() {
        running = false;
        if (streams != null) streams.close(Duration.ofSeconds(10));
        if (writer != null) {
            try { writer.join(8000); if (writer.isAlive()) { writer.interrupt(); writer.join(2000); } }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }
}
