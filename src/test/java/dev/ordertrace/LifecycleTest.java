package dev.ordertrace;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static dev.ordertrace.OrderEvent.Source.*;
import static dev.ordertrace.OrderEvent.Type.*;

class LifecycleTest {
    static List<OrderEvent> demo() throws Exception {
        return Files.readAllLines(Path.of("data/demo.ndjson")).stream().map(line -> Json.read(line, OrderEvent.class)).toList();
    }
    static Aggregate aggregate(List<OrderEvent> events, String root) {
        Aggregate a = new Aggregate(); events.stream().filter(e -> e.rootOrderId().equals(root)).forEach(a::add); return a;
    }
    static OrderEvent event(String id, OrderEvent.Type type, long sequence, Long quantity, Long fill) {
        return new OrderEvent(id,"P","C","P", type == CHILD_CREATED ? SCEPTER : EG, type, Instant.parse("2026-01-01T00:00:00Z"),
                sequence, quantity, quantity == null ? null : new BigDecimal("10.25"), fill, fill == null ? null : id, null);
    }
    @Test void topologyHandlesDuplicatesOutOfOrderAndAuthority() throws Exception {
        Properties config = new Properties(); config.put(StreamsConfig.APPLICATION_ID_CONFIG,"unit");config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"dummy:9092");
        try (TopologyTestDriver driver = new TopologyTestDriver(LifecycleTopology.build("raw","results"),config)) {
            var input = driver.createInputTopic("raw",new StringSerializer(),Json.serde(OrderEvent.class).serializer());
            var output = driver.createOutputTopic("results",new org.apache.kafka.common.serialization.StringDeserializer(),Json.serde(Aggregate.Snapshot.class).deserializer());
            for (OrderEvent event : demo()) input.pipeInput(event.rootOrderId(),event);
            var results = output.readKeyValuesToMap();
            assertThat(results.get("DEMO-1001").filledQuantity()).isEqualTo(100);
            assertThat(results.get("DEMO-1001").status()).isEqualTo("FILLED");
            assertThat(results.get("DEMO-1001").version()).isEqualTo(15);
            assertThat(results.get("DEMO-1001").warnings()).isEmpty();
            var replaced = results.get("DEMO-1001").children().get(1);
            assertThat(replaced.price()).isEqualByComparingTo("125.6");
            assertThat(replaced.pendingCancel()).isFalse(); assertThat(replaced.pendingReplace()).isFalse();
            assertThat(results.get("DEMO-1002").children().getFirst().status()).isEqualTo("CANCELED");
            assertThat(results.get("DEMO-1002").filledQuantity()).isEqualTo(25);
            assertThat(results.get("DEMO-1003").warnings()).contains("MISSING_PARENT_CREATED","CHILD_INFORMATION_INCOMPLETE");
            assertThat(results.get("DEMO-1004").status()).isEqualTo("ACTIVE");
            assertThat(results.get("DEMO-1004").filledQuantity()).isEqualTo(50);
        }
    }
    @Test void permutationsConvergeWithoutTimestampOrdering() throws Exception {
        var evidence = new ArrayList<>(demo());
        var expected = aggregate(evidence,"DEMO-1001").snapshot();
        for(int seed=0;seed<30;seed++) {
            Collections.shuffle(evidence,new Random(seed));
            assertThat(aggregate(evidence,"DEMO-1001").snapshot()).isEqualTo(expected);
        }
    }
    @Test void rejectedRequestsPreserveExecutionStateAndPrice() {
        Aggregate a = new Aggregate();
        a.add(event("create",CHILD_CREATED,1,100L,null));a.add(event("ack",ACK,1,null,null));
        a.add(event("fill",FILL,2,null,20L));a.add(event("cancel",CANCEL_REQUESTED,3,null,null));
        assertThat(a.snapshot().children().getFirst().pendingCancel()).isTrue();
        a.add(event("cancel-no",CANCEL_REJECTED,4,null,null));
        a.add(event("replace",REPLACE_REQUESTED,5,120L,null));a.add(event("replace-no",REPLACE_REJECTED,6,null,null));
        var child = a.snapshot().children().getFirst();
        assertThat(child.status()).isEqualTo("PARTIALLY_FILLED"); assertThat(child.quantity()).isEqualTo(100);
        assertThat(child.price()).isEqualByComparingTo("10.25");assertThat(child.pendingCancel()).isFalse();assertThat(child.pendingReplace()).isFalse();
    }
    @Test void latePredecessorsRepairWarningsWithoutRegressingTerminalState() {
        Aggregate a = new Aggregate();a.add(event("fill",FILL,2,null,100L));
        assertThat(a.snapshot().children().getFirst().warnings()).contains("MISSING_CHILD_CREATED","MISSING_ACK");
        a.add(event("create",CHILD_CREATED,1,100L,null));a.add(event("ack",ACK,1,null,null));
        assertThat(a.snapshot().children().getFirst().status()).isEqualTo("FILLED");
        assertThat(a.snapshot().children().getFirst().warnings()).isEmpty();
        a.add(event("late-ack",ACK,3,null,null));
        assertThat(a.snapshot().children().getFirst().status()).isEqualTo("FILLED");
    }
    @Test void deduplicatesExecutionFactsAndFailsOnConflictingEvidence() {
        Aggregate a = new Aggregate(); var fill = event("f",FILL,1,null,20L);a.add(fill);a.add(fill);
        assertThat(a.version).isEqualTo(1);
        a.add(new OrderEvent("relay","P","C","P",EG,FILL,fill.occurredAt(),2,null,null,20L,"f",null));
        assertThat(a.snapshot().filledQuantity()).isEqualTo(20);
        assertThatThrownBy(() -> a.add(event("different",FILL,1,null,20L))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> a.add(event("f",FILL,1,null,30L))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderEvent("bad","P","P",null,EG,PARENT_FINAL,fill.occurredAt(),1,null,null,null,null,"FILLED").validate()).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderEvent("relay","P","C","P",SCEPTER,FILL,fill.occurredAt(),1,null,null,20L,"f",null).validate()).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void replaceKeepsLogicalIdAndDoesNotReopenTerminalOrder() {
        Aggregate a = new Aggregate();a.add(event("create",CHILD_CREATED,1,100L,null));a.add(event("ack",ACK,1,null,null));
        a.add(event("request",REPLACE_REQUESTED,2,150L,null));a.add(event("accept",REPLACE_ACCEPTED,3,150L,null));
        assertThat(a.snapshot().children()).hasSize(1); assertThat(a.snapshot().children().getFirst().quantity()).isEqualTo(150);
        a.add(event("fill",FILL,4,null,150L));a.add(event("too-late",REPLACE_ACCEPTED,5,200L,null));
        assertThat(a.snapshot().children().getFirst().status()).isEqualTo("FILLED");
        assertThat(a.snapshot().children().getFirst().quantity()).isEqualTo(150);
    }
}
