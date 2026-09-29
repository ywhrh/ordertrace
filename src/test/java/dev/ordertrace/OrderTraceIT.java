package dev.ordertrace;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrderTraceIT {
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.2");
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11");
    static final String RUN = "it-" + UUID.randomUUID();
    @DynamicPropertySource static void configuration(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",POSTGRES::getUsername);registry.add("spring.datasource.password",POSTGRES::getPassword);
        registry.add("ordertrace.bootstrap",KAFKA::getBootstrapServers);registry.add("ordertrace.application-id",()->RUN);
        registry.add("ordertrace.state-dir",()->"target/"+RUN);registry.add("ordertrace.schema",()->"integration");
        registry.add("spring.flyway.schemas",()->"integration");registry.add("spring.flyway.default-schema",()->"integration");
    }
    @Autowired Settings settings;
    @Autowired ReadModel model;
    @Autowired Pipeline pipeline;
    @Autowired JdbcTemplate jdbc;
    @Autowired TestRestTemplate http;

    @Test @Order(1) void realKafkaToPostgresAndRest() throws Exception {
        Commands.publish(settings,Path.of("data/demo.ndjson"));
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(model.parents(null,null,100,0)).hasSize(4);
            assertThat(model.parent("DEMO-1001").orElseThrow().get("version")).isEqualTo(15L);
            assertThat(model.parent("DEMO-1002").orElseThrow().get("version")).isEqualTo(7L);
        });
        assertThat(model.parent("DEMO-1001").orElseThrow().get("filled_quantity")).isEqualTo(100L);
        assertThat(model.events("DEMO-1001",null,100,0)).hasSize(15);
        assertThat(model.children("DEMO-1001",100,0)).hasSize(2);
        var filtered = Json.MAPPER.readTree(http.getForEntity("/api/parents?status=FILLED&limit=1",String.class).getBody());
        assertThat(filtered.get("items").size()).isEqualTo(1);
        assertThat(filtered.get("items").get(0).get("root_order_id").asText()).isEqualTo("DEMO-1001");
        var secondPage = Json.MAPPER.readTree(http.getForEntity("/api/parents?limit=1&offset=1",String.class).getBody());
        assertThat(secondPage.get("items").get(0).get("root_order_id").asText()).isEqualTo("DEMO-1002");
        assertThat(http.getForEntity("/api/parents/DEMO-1001/children",String.class).getBody()).contains("125.600000");
        var timeline = Json.MAPPER.readTree(http.getForEntity("/api/parents/DEMO-1001/events?childOrderId=DEMO-1001-A",String.class).getBody());
        assertThat(timeline.get("items").size()).isEqualTo(4);
        timeline.get("items").forEach(event -> assertThat(event.get("order_id").asText()).isEqualTo("DEMO-1001-A"));
        assertThat(http.getForEntity("/",String.class).getBody()).contains("Follow every order");
        assertThat(http.getForEntity("/api/parents/missing",String.class).getStatusCode().value()).isEqualTo(404);
        for (String query : List.of("limit=101","limit=0","offset=-1","offset=10001","limit=nope","status=INVALID","rootOrderId=bad!"))
            assertThat(http.getForEntity("/api/parents?"+query,String.class).getStatusCode().value()).isEqualTo(400);
    }
    @Test @Order(2) void databaseRetriesStaleVersionsAndRollbackAreSafe() throws Exception {
        Aggregate a = LifecycleTest.aggregate(LifecycleTest.demo(),"DEMO-1001");
        Object before = model.businessView();
        assertThat(model.persist(a.snapshot())).isFalse();
        Aggregate stale = new Aggregate();stale.add(a.events.firstEntry().getValue());
        assertThat(model.persist(stale.snapshot())).isFalse();
        assertThat(model.businessView()).isEqualTo(before);
        var s = a.snapshot();var events = new ArrayList<>(s.events());var original = events.getFirst();
        events.add(new OrderEvent("conflicting-sequence",original.rootOrderId(),original.orderId(),original.parentOrderId(),original.source(),original.eventType(),original.occurredAt(),original.sequence(),original.quantity(),original.price(),original.fillQuantity(),original.executionId(),original.finalStatus()));
        var invalid = new Aggregate.Snapshot(s.rootOrderId(),s.version()+1,s.status(),s.quantity(),999,s.updatedAt(),s.warnings(),s.children(),events);
        assertThatThrownBy(() -> model.persist(invalid)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(model.businessView()).isEqualTo(before);
        assertThat(model.parent("DEMO-1001").orElseThrow().get("version")).isEqualTo(15L);
    }
    @Test @Order(3) void sameApplicationIdRestartsAndContinuesWithoutDoubleCounting() throws Exception {
        pipeline.stop(); pipeline.start();
        // A new envelope for an existing execution proves restored evidence before republishing history.
        var afterRestart = new OrderEvent("resumed-execution-envelope","DEMO-1001","DEMO-1001-B","DEMO-1001",
                OrderEvent.Source.EG,OrderEvent.Type.FILL,Instant.parse("2026-01-15T14:30:59Z"),9,null,null,40L,"exec-1001-B-1",null);
        Path resumed = Path.of("target/resumed.ndjson"); java.nio.file.Files.writeString(resumed,Json.write(afterRestart)+"\n");
        Commands.publish(settings,resumed);
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(model.parent("DEMO-1001").orElseThrow().get("version")).isEqualTo(16L);
            assertThat(model.parent("DEMO-1001").orElseThrow().get("filled_quantity")).isEqualTo(100L);
            assertThat(model.events("DEMO-1001",null,100,0)).hasSize(16);
        });
        Object before = model.businessView();
        Commands.publish(settings,Path.of("data/demo.ndjson"));
        try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers",settings.bootstrap()))) {
            var ends = Commands.offsets(admin, settings.rawTopic(), org.apache.kafka.clients.admin.OffsetSpec.latest());
            await().atMost(Duration.ofSeconds(60)).until(() -> Commands.caughtUp(admin,settings.applicationId(),ends));
        }
        assertThat(model.businessView()).isEqualTo(before);
    }
    @Test @Order(4) void unavailableDatabaseRetriesBufferedResults() throws Exception {
        POSTGRES.getDockerClient().pauseContainerCmd(POSTGRES.getContainerId()).exec();
        try {
            var event = new OrderEvent("final-1004","DEMO-1004","DEMO-1004",null,OrderEvent.Source.SCEPTER,OrderEvent.Type.PARENT_FINAL,
                    Instant.parse("2026-01-15T14:31:00Z"),1,null,null,null,null,"FILLED");
            Path data = Path.of("target/outage.ndjson"); java.nio.file.Files.writeString(data,Json.write(event)+"\n");
            Commands.publish(settings,data);
            await().atMost(Duration.ofSeconds(40)).until(() -> pipeline.status().get("writer").equals("RETRYING"));
        } finally { POSTGRES.getDockerClient().unpauseContainerCmd(POSTGRES.getContainerId()).exec(); }
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            assertThat(model.parent("DEMO-1004").orElseThrow().get("status")).isEqualTo("FILLED");
            assertThat(model.parent("DEMO-1004").orElseThrow().get("filled_quantity")).isEqualTo(50L);
        });
    }
    @Test @Order(5) void isolatedReplayProducesSamePostgresBusinessResults() throws Exception {
        String namespace = "integration_replay";
        org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())
                .schemas(namespace).defaultSchema(namespace).load().migrate();
        Settings replaySettings = new Settings(namespace,settings.bootstrap(),RUN+"-replay",settings.rawTopic(),"it-results-replay","target/"+RUN+"-replay",true);
        var replayModel = new ReadModel(jdbc,new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(jdbc.getDataSource())),replaySettings);
        Pipeline replay = new Pipeline(replaySettings,replayModel);
        try {
            replay.start();
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(replayModel.businessView()).isEqualTo(model.businessView()));
        } finally { replay.stop(); }
    }
}
