package dev.ordertrace;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

/** Sequence scope is (source, orderId), stable for the lifetime of a logical order. */
public record OrderEvent(String eventId, String rootOrderId, String orderId, String parentOrderId,
                         Source source, Type eventType, Instant occurredAt, long sequence,
                         Long quantity, BigDecimal price, Long fillQuantity, String executionId,
                         String finalStatus) {
    public enum Source { CG, SCEPTER, EG }
    public enum Type { PARENT_CREATED, CHILD_CREATED, ACK, FILL, CANCEL_REQUESTED, CANCEL_ACCEPTED,
        CANCEL_REJECTED, REPLACE_REQUESTED, REPLACE_ACCEPTED, REPLACE_REJECTED, PARENT_FINAL }

    public void validate() {
        id(eventId, 100); id(rootOrderId, 80); id(orderId, 80);
        if (source == null || eventType == null || occurredAt == null || sequence < 1)
            throw new IllegalArgumentException("source, eventType, occurredAt and positive sequence are required");
        boolean parent = eventType == Type.PARENT_CREATED || eventType == Type.PARENT_FINAL;
        if (parent ? !orderId.equals(rootOrderId) || parentOrderId != null
                   : orderId.equals(rootOrderId) || !rootOrderId.equals(parentOrderId))
            throw new IllegalArgumentException("Invalid parent/child relationship");
        Source authority = switch (eventType) {
            case PARENT_CREATED -> Source.CG;
            case CHILD_CREATED, PARENT_FINAL -> Source.SCEPTER;
            default -> Source.EG;
        };
        if (source != authority) throw new IllegalArgumentException("Non-authoritative event source for " + eventType);
        if (quantity != null && (quantity < 1 || quantity > 1_000_000_000L))
            throw new IllegalArgumentException("quantity must be 1..1000000000");
        if (price != null && (price.signum() <= 0 || price.scale() > 6 || price.precision() - price.scale() > 14))
            throw new IllegalArgumentException("price must be positive numeric(20,6)");
        if ((eventType == Type.PARENT_CREATED || eventType == Type.CHILD_CREATED || eventType == Type.REPLACE_ACCEPTED)
                && quantity == null) throw new IllegalArgumentException("quantity required");
        if ((eventType == Type.CHILD_CREATED || eventType == Type.REPLACE_ACCEPTED) && price == null)
            throw new IllegalArgumentException("price required");
        if (eventType == Type.FILL) {
            id(executionId, 100);
            if (fillQuantity == null || fillQuantity < 1 || fillQuantity > 1_000_000_000L)
                throw new IllegalArgumentException("fillQuantity must be 1..1000000000");
        } else if (executionId != null || fillQuantity != null) throw new IllegalArgumentException("fill fields only allowed on FILL");
        if (eventType == Type.PARENT_FINAL) {
            if (!Set.of("FILLED", "CANCELED", "REJECTED", "COMPLETED").contains(finalStatus == null ? "" : finalStatus))
                throw new IllegalArgumentException("Invalid Scepter finalStatus");
        } else if (finalStatus != null) throw new IllegalArgumentException("finalStatus only allowed on PARENT_FINAL");
    }

    public static void id(String value, int max) {
        if (value == null || value.length() > max || !value.matches("[A-Za-z0-9][A-Za-z0-9_.-]*"))
            throw new IllegalArgumentException("Invalid identifier (letters, digits, _, ., - only; max " + max + ")");
    }
}
