package dev.ordertrace;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import java.sql.Timestamp;
import java.util.*;

/** Only committed snapshots become visible; root version serializes concurrent/retried writes. */
@Repository
public class ReadModel {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final String schema;
    public ReadModel(JdbcTemplate jdbc, TransactionTemplate transactions, Settings settings) {
        this.jdbc = jdbc; this.transactions = transactions; this.schema = settings.schema();
    }
    public boolean persist(Aggregate.Snapshot snapshot) {
        return Boolean.TRUE.equals(transactions.execute(tx -> {
            int changed = jdbc.update("""
                INSERT INTO %s.parents(root_order_id,status,quantity,filled_quantity,child_count,version,business_updated_at,warnings)
                VALUES(?,?,?,?,?,?,?,?::jsonb)
                ON CONFLICT(root_order_id) DO UPDATE SET status=excluded.status, quantity=excluded.quantity,
                filled_quantity=excluded.filled_quantity, child_count=excluded.child_count, version=excluded.version,
                business_updated_at=excluded.business_updated_at, warnings=excluded.warnings, persisted_at=clock_timestamp()
                WHERE parents.version < excluded.version
                """.formatted(schema), snapshot.rootOrderId(), snapshot.status(), snapshot.quantity(), snapshot.filledQuantity(),
                    snapshot.children().size(), snapshot.version(), Timestamp.from(snapshot.updatedAt()), Json.write(snapshot.warnings()));
            if (changed == 0) return false;
            for (Aggregate.Child child : snapshot.children()) jdbc.update("""
                INSERT INTO %s.children(root_order_id,order_id,status,quantity,filled_quantity,price,pending_cancel,pending_replace,warnings)
                VALUES(?,?,?,?,?,?,?,?,?::jsonb) ON CONFLICT(root_order_id,order_id) DO UPDATE SET
                status=excluded.status, quantity=excluded.quantity, filled_quantity=excluded.filled_quantity,
                price=excluded.price, pending_cancel=excluded.pending_cancel, pending_replace=excluded.pending_replace,warnings=excluded.warnings
                """.formatted(schema), snapshot.rootOrderId(), child.orderId(), child.status(), child.quantity(), child.filledQuantity(),
                    child.price(), child.pendingCancel(), child.pendingReplace(), Json.write(child.warnings()));
            for (OrderEvent event : snapshot.events()) jdbc.update("""
                INSERT INTO %s.events(root_order_id,event_id,order_id,source,event_type,occurred_at,source_sequence,payload)
                VALUES(?,?,?,?,?,?,?,?::jsonb) ON CONFLICT(root_order_id,event_id) DO NOTHING
                """.formatted(schema), snapshot.rootOrderId(), event.eventId(), event.orderId(), event.source().name(), event.eventType().name(),
                    Timestamp.from(event.occurredAt()), event.sequence(), Json.write(event));
            return true;
        }));
    }
    private List<Map<String,Object>> rows(String sql, Object... arguments) {
        return jdbc.query(sql, (rs, row) -> {
            Map<String,Object> item = new LinkedHashMap<>();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String name = rs.getMetaData().getColumnLabel(i);
                Object value = rs.getObject(i);
                if (value instanceof Timestamp timestamp) value = timestamp.toInstant().toString();
                if ((name.equals("warnings") || name.equals("payload")) && value != null) value = Json.read(value.toString(), Object.class);
                item.put(name, value);
            }
            return item;
        }, arguments);
    }
    public List<Map<String,Object>> parents(String root, String status, int limit, int offset) {
        return rows("SELECT * FROM " + schema + ".parents WHERE (?::text IS NULL OR root_order_id=?) AND (?::text IS NULL OR status=?) ORDER BY root_order_id LIMIT ? OFFSET ?",
                root, root, status, status, limit, offset);
    }
    public Optional<Map<String,Object>> parent(String root) { return parents(root, null, 1, 0).stream().findFirst(); }
    public List<Map<String,Object>> children(String root, int limit, int offset) {
        return rows("SELECT * FROM " + schema + ".children WHERE root_order_id=? ORDER BY order_id LIMIT ? OFFSET ?", root, limit, offset);
    }
    public List<Map<String,Object>> events(String root, String child, int limit, int offset) {
        return rows("SELECT * FROM " + schema + ".events WHERE root_order_id=? AND (?::text IS NULL OR order_id=?) ORDER BY occurred_at,event_id LIMIT ? OFFSET ?", root, child, child, limit, offset);
    }
    /** Canonical database content for replay comparison; excludes processing time and technical version. */
    public Map<String,Object> businessView() {
        return Map.of("parents", rows("SELECT root_order_id,status,quantity,filled_quantity,child_count,business_updated_at,warnings FROM " + schema + ".parents ORDER BY root_order_id"),
                "children", rows("SELECT * FROM " + schema + ".children ORDER BY root_order_id,order_id"),
                "events", rows("SELECT * FROM " + schema + ".events ORDER BY root_order_id,event_id"));
    }
}
