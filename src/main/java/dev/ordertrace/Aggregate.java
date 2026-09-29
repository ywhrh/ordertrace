package dev.ordertrace;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import static dev.ordertrace.OrderEvent.Type.*;

/** Small-demo reducer: retain bounded evidence and recompute per-authority sequences on late arrival. */
public class Aggregate {
    public String rootOrderId;
    public long version;
    public TreeMap<String, OrderEvent> events = new TreeMap<>();

    public Aggregate add(OrderEvent event) {
        event.validate();
        if (rootOrderId != null && !rootOrderId.equals(event.rootOrderId())) throw new IllegalArgumentException("Mixed roots");
        OrderEvent existing = events.get(event.eventId());
        if (existing != null) {
            if (!existing.equals(event)) throw new IllegalArgumentException("Conflicting eventId: " + event.eventId());
            return this;
        }
        if (events.values().stream().anyMatch(e -> e.source() == event.source() && e.orderId().equals(event.orderId()) && e.sequence() == event.sequence()))
            throw new IllegalArgumentException("Conflicting source-local sequence");
        // ponytail: full evidence is O(n) per update; capped at 500 events/root. Use incremental state for larger workloads.
        if (events.size() >= 500) throw new IllegalStateException("Demo limit: 500 unique events per root");
        rootOrderId = event.rootOrderId();
        events.put(event.eventId(), event);
        version++;
        return this;
    }

    public record Child(String orderId, String status, Long quantity, long filledQuantity, BigDecimal price,
                        boolean pendingCancel, boolean pendingReplace, List<String> warnings) { }
    public record Snapshot(String rootOrderId, long version, String status, Long quantity, long filledQuantity,
                           Instant updatedAt, List<String> warnings, List<Child> children, List<OrderEvent> events) { }

    public Snapshot snapshot() {
        List<OrderEvent> all = new ArrayList<>(events.values());
        TreeSet<String> warnings = new TreeSet<>();
        List<OrderEvent> parent = all.stream().filter(e -> e.orderId().equals(rootOrderId)).toList();
        OrderEvent created = parent.stream().filter(e -> e.eventType() == PARENT_CREATED).min(Comparator.comparingLong(OrderEvent::sequence)).orElse(null);
        if (created == null) warnings.add("MISSING_PARENT_CREATED");
        if (parent.stream().filter(e -> e.eventType() == PARENT_CREATED).count() > 1) warnings.add("MULTIPLE_PARENT_CREATED");
        OrderEvent terminal = parent.stream().filter(e -> e.eventType() == PARENT_FINAL).max(Comparator.comparingLong(OrderEvent::sequence)).orElse(null);
        String status = terminal != null ? terminal.finalStatus() : created != null ? "ACTIVE" : "UNKNOWN";
        if (terminal == null) warnings.add("AWAITING_SCEPTER_FINAL");
        List<Child> children = all.stream().filter(e -> !e.orderId().equals(rootOrderId)).map(OrderEvent::orderId).distinct().sorted()
                .map(id -> child(id, all.stream().filter(e -> e.orderId().equals(id)).toList())).toList();
        if (children.stream().anyMatch(c -> !c.warnings().isEmpty())) warnings.add("CHILD_INFORMATION_INCOMPLETE");
        long filled = children.stream().mapToLong(Child::filledQuantity).sum();
        if (created != null && filled > created.quantity()) warnings.add("PARENT_OVERFILLED");
        if ("FILLED".equals(status) && (created == null || filled != created.quantity())) warnings.add("FINAL_FILL_TOTAL_MISMATCH");
        checkSequences(parent, warnings);
        // Timestamp is display metadata, never the cross-service ordering authority.
        Instant updated = all.stream().map(OrderEvent::occurredAt).max(Comparator.naturalOrder()).orElseThrow();
        all.sort(Comparator.comparing(OrderEvent::occurredAt).thenComparing(OrderEvent::eventId));
        return new Snapshot(rootOrderId, version, status, created == null ? null : created.quantity(), filled, updated,
                List.copyOf(warnings), children, all);
    }

    private static Child child(String id, List<OrderEvent> events) {
        TreeSet<String> warnings = new TreeSet<>();
        OrderEvent creation = events.stream().filter(e -> e.eventType() == CHILD_CREATED).min(Comparator.comparingLong(OrderEvent::sequence)).orElse(null);
        if (creation == null) warnings.add("MISSING_CHILD_CREATED");
        if (events.stream().filter(e -> e.eventType() == CHILD_CREATED).count() > 1) warnings.add("MULTIPLE_CHILD_CREATED");
        Long quantity = creation == null ? null : creation.quantity();
        BigDecimal price = creation == null ? null : creation.price();
        long filled = 0;
        boolean ack = false, cancel = false, replace = false, canceled = false;
        Map<String, Long> executions = new HashMap<>();
        for (OrderEvent event : events.stream().filter(e -> e.source() == OrderEvent.Source.EG).sorted(Comparator.comparingLong(OrderEvent::sequence)).toList()) {
            boolean ended = canceled || (quantity != null && filled >= quantity);
            switch (event.eventType()) {
                case ACK -> { ack = true; if (ended) warnings.add("ACK_AFTER_TERMINAL"); }
                case FILL -> {
                    Long prior = executions.putIfAbsent(event.executionId(), event.fillQuantity());
                    if (prior == null) filled += event.fillQuantity();
                    else if (!prior.equals(event.fillQuantity())) throw new IllegalArgumentException("Conflicting executionId");
                }
                case CANCEL_REQUESTED -> { if (!ended) cancel = true; else warnings.add("REQUEST_AFTER_TERMINAL"); }
                case CANCEL_ACCEPTED -> { if (!cancel) warnings.add("MISSING_CANCEL_REQUEST"); canceled = true; cancel = false; replace = false; }
                case CANCEL_REJECTED -> { if (!cancel) warnings.add("MISSING_CANCEL_REQUEST"); cancel = false; }
                case REPLACE_REQUESTED -> { if (!ended) replace = true; else warnings.add("REQUEST_AFTER_TERMINAL"); }
                case REPLACE_ACCEPTED -> {
                    if (!replace) warnings.add("MISSING_REPLACE_REQUEST");
                    if (ended || event.quantity() < filled) warnings.add("INVALID_REPLACE_AFTER_FILL_OR_TERMINAL");
                    else { quantity = event.quantity(); price = event.price(); }
                    replace = false;
                }
                case REPLACE_REJECTED -> { if (!replace) warnings.add("MISSING_REPLACE_REQUEST"); replace = false; }
                default -> throw new IllegalArgumentException("Unexpected child authority");
            }
        }
        if (!ack) warnings.add("MISSING_ACK");
        checkSequences(events, warnings);
        if (quantity != null && filled > quantity) warnings.add("CHILD_OVERFILLED");
        String status = quantity != null && filled >= quantity ? "FILLED" : canceled ? "CANCELED"
                : filled > 0 ? "PARTIALLY_FILLED" : ack ? "ACKNOWLEDGED" : creation != null ? "CREATED" : "UNKNOWN";
        if (status.equals("FILLED")) { cancel = false; replace = false; }
        return new Child(id, status, quantity, filled, price, cancel, replace, List.copyOf(warnings));
    }

    private static void checkSequences(List<OrderEvent> events, Set<String> warnings) {
        Map<String, List<Long>> scopes = new TreeMap<>();
        for (OrderEvent event : events) scopes.computeIfAbsent(event.source() + ":" + event.orderId(), k -> new ArrayList<>()).add(event.sequence());
        scopes.forEach((scope, sequences) -> {
            sequences.sort(Long::compareTo);
            long expected = 1;
            for (long sequence : sequences) { if (sequence != expected) warnings.add("SEQUENCE_GAP:" + scope); expected = sequence + 1; }
        });
    }
}
