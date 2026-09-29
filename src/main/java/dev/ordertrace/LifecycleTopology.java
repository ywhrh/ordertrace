package dev.ordertrace;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

public final class LifecycleTopology {
    public static Topology build(String rawTopic, String resultTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(rawTopic, Consumed.with(Serdes.String(), Json.serde(OrderEvent.class)))
                .peek((key, event) -> {
                    if (event == null) throw new IllegalArgumentException("Tombstones unsupported");
                    event.validate();
                    if (!event.rootOrderId().equals(key)) throw new IllegalArgumentException("Kafka key must equal rootOrderId");
                })
                .groupByKey(Grouped.with(Serdes.String(), Json.serde(OrderEvent.class)))
                .aggregate(Aggregate::new, (root, event, aggregate) -> aggregate.add(event),
                        Materialized.<String, Aggregate, KeyValueStore<Bytes, byte[]>>as("order-evidence")
                                .withKeySerde(Serdes.String()).withValueSerde(Json.serde(Aggregate.class)))
                .toStream().mapValues(Aggregate::snapshot)
                .to(resultTopic, Produced.with(Serdes.String(), Json.serde(Aggregate.Snapshot.class)));
        return builder.build();
    }
}
