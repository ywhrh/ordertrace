package dev.ordertrace;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import java.nio.file.*;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Local operator commands share the same topology, migrations and transactional writer as the web app. */
final class Commands {
    static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
    static Settings settings(String schema, String app, String result) {
        return new Settings(schema, env("KAFKA_BOOTSTRAP", "localhost:9092"), app,
                "ordertrace.events.v2", result, env("OT_STATE_DIR", ".runtime/streams"), true);
    }
    static Settings live() { return settings(env("OT_SCHEMA", "ordertrace_v2"), env("OT_APPLICATION_ID", "ordertrace-live-v2"), env("OT_RESULT_TOPIC", "ordertrace.results.v2")); }
    static void run(String[] args) throws Exception {
        switch (args[0]) {
            case "publish" -> publish(live(), Path.of(args.length > 1 ? args[1] : "data/demo.ndjson"));
            case "replay" -> { if (args.length != 2) throw new IllegalArgumentException("Usage: replay replay_unique_name"); replay(args[1]); }
            case "compare" -> { if (args.length != 2) throw new IllegalArgumentException("Usage: compare replay_unique_name"); compare(args[1]); }
            default -> throw new IllegalArgumentException("Commands: publish [file.ndjson], replay replay_unique_name, compare replay_unique_name");
        }
    }
    static void publish(Settings settings, Path path) throws Exception {
        List<OrderEvent> events = Files.readAllLines(path).stream().filter(line -> !line.isBlank()).map(line -> Json.read(line, OrderEvent.class)).toList();
        events.forEach(OrderEvent::validate);
        Pipeline.topics(settings);
        Map<OrderEvent.Source, KafkaProducer<String,String>> publishers = new EnumMap<>(OrderEvent.Source.class);
        try {
            for (OrderEvent.Source source : OrderEvent.Source.values()) publishers.put(source, new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrap(), ProducerConfig.CLIENT_ID_CONFIG, "simulator-" + source,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.ACKS_CONFIG, "all", ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)));
            for (OrderEvent event : events) publishers.get(event.source()).send(new ProducerRecord<>(settings.rawTopic(), event.rootOrderId(), Json.write(event))).get(30, TimeUnit.SECONDS);
            System.out.println("Published " + events.size() + " records via CG/SCEPTER/EG; stable IDs make repeats idempotent.");
        } finally { publishers.values().forEach(p -> p.close(Duration.ofSeconds(5))); }
    }
    static ConfigurableApplicationContext context(Settings settings, boolean pipeline) {
        return SpringApplication.run(OrderTrace.class,
                "--spring.main.web-application-type=none", "--ordertrace.pipeline-enabled=" + pipeline,
                "--ordertrace.schema=" + settings.schema(), "--spring.flyway.schemas=" + settings.schema(),
                "--spring.flyway.default-schema=" + settings.schema(), "--ordertrace.application-id=" + settings.applicationId(),
                "--ordertrace.result-topic=" + settings.resultTopic());
    }
    static void namespace(String namespace) {
        if (!namespace.matches("replay_[a-z0-9_]{1,40}") || namespace.equals(live().schema()))
            throw new IllegalArgumentException("Use a fresh replay_[a-z0-9_] namespace, max 47 chars");
    }
    static boolean schemaExists(String schema) throws Exception {
        try (var connection = DriverManager.getConnection(env("DB_URL", "jdbc:postgresql://localhost:5432/ordertrace"),
                env("DB_USER", "ordertrace"), env("DB_PASSWORD", "ordertrace_demo"));
             var query = connection.prepareStatement("SELECT 1 FROM information_schema.schemata WHERE schema_name=?")) {
            query.setString(1, schema);
            try (var result = query.executeQuery()) { return result.next(); }
        }
    }
    static Map<TopicPartition,Long> offsets(Admin admin, String topic, OffsetSpec spec) throws Exception {
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get(15, TimeUnit.SECONDS).get(topic);
        Map<TopicPartition,OffsetSpec> request = new HashMap<>();
        description.partitions().forEach(p -> request.put(new TopicPartition(topic, p.partition()), spec));
        Map<TopicPartition,Long> result = new HashMap<>();
        admin.listOffsets(request).all().get(15, TimeUnit.SECONDS).forEach((p, offset) -> result.put(p, offset.offset()));
        return result;
    }
    static boolean caughtUp(Admin admin, String group, Map<TopicPartition,Long> ends) throws Exception {
        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
        return ends.entrySet().stream().allMatch(e -> e.getValue() == 0 || committed.containsKey(e.getKey()) && committed.get(e.getKey()).offset() >= e.getValue());
    }
    static void replay(String namespace) throws Exception {
        namespace(namespace);
        Settings live = live();
        Settings replay = settings(namespace, "ordertrace-" + namespace, "ordertrace.results." + namespace);
        if (schemaExists(namespace)) throw new IllegalArgumentException("Replay schema exists; choose a new namespace");
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", live.bootstrap()))) {
            Set<String> topics = admin.listTopics(new ListTopicsOptions().listInternal(true)).names().get(15, TimeUnit.SECONDS);
            if (topics.contains(replay.resultTopic()) || topics.stream().anyMatch(t -> t.startsWith(replay.applicationId() + "-"))
                    || admin.listConsumerGroups().all().get(15, TimeUnit.SECONDS).stream().anyMatch(g -> g.groupId().startsWith(replay.applicationId())))
                throw new IllegalArgumentException("Replay Kafka namespace exists; choose a new one");
            Map<TopicPartition,Long> start = offsets(admin, live.rawTopic(), OffsetSpec.earliest());
            Map<TopicPartition,Long> end = offsets(admin, live.rawTopic(), OffsetSpec.latest());
            System.out.println("Replaying retained raw offset range: " + start + " -> " + end + ". Keep publishers stopped.");
            try (var context = context(replay, true)) {
                long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
                int stable = 0;
                while (System.nanoTime() < deadline) {
                    Thread.sleep(1000);
                    if (!end.equals(offsets(admin, live.rawTopic(), OffsetSpec.latest())) || !start.equals(offsets(admin, live.rawTopic(), OffsetSpec.earliest())))
                        throw new IllegalStateException("Raw offset range changed during replay; stop publishers / check retention and retry with a fresh namespace");
                    if (caughtUp(admin, replay.applicationId(), end)
                            && caughtUp(admin, replay.writerGroup(), offsets(admin, replay.resultTopic(), OffsetSpec.latest()))) stable++;
                    else stable = 0;
                    if (stable >= 3) { System.out.println("Replay drained to PostgreSQL schema " + namespace); break; }
                }
                if (stable < 3) throw new IllegalStateException("Replay timeout: inspect stream/writer logs");
            }
        }
        compare(namespace);
    }
    static void compare(String namespace) throws Exception {
        namespace(namespace);
        if (!schemaExists(live().schema()) || !schemaExists(namespace)) throw new IllegalArgumentException("Both database schemas must already exist");
        Object normal, rebuilt;
        try (var context = context(live(), false)) { normal = context.getBean(ReadModel.class).businessView(); }
        try (var context = context(settings(namespace, "ordertrace-" + namespace, "ordertrace.results." + namespace), false)) {
            rebuilt = context.getBean(ReadModel.class).businessView();
        }
        if (!Json.MAPPER.valueToTree(normal).equals(Json.MAPPER.valueToTree(rebuilt)))
            throw new IllegalStateException("Business comparison FAILED. Live may be catching up, raw history may have expired, or behavior differs. Isolated data retained for inspection.");
        System.out.println("PASS: PostgreSQL parents, children and complete event payloads match (excluding persistence time/version).");
    }
}
