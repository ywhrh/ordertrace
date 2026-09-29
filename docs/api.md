# REST API

Base URL: `http://localhost:8080`. Read-only JSON endpoints, all business reads from PostgreSQL. IDs are case-sensitive. Query/path identifiers permit letters, digits, `_`, `-`, `.`, start with an alphanumeric character and have at most 80 characters.

| Method/path | Query parameters | Behavior |
|---|---|---|
| `GET /api/parents` | optional exact `rootOrderId`, optional `status`, `limit` default 20, `offset` default 0 | Ordered by root ID |
| `GET /api/parents/{root}` | none | Parent summary; 404 when absent |
| `GET /api/parents/{root}/children` | `limit` default 100, `offset` default 0 | Ordered by logical child ID; parent 404 when absent |
| `GET /api/parents/{root}/events` | optional exact `childOrderId`, `limit` default 50, `offset` default 0 | Ordered by `occurred_at,event_id`; unknown child filter returns empty list |
| `GET /api/health` | none | Checks DB query, Streams state and writer state; 503 when unavailable/unready |

All paginated endpoints enforce `1 <= limit <= 100`, `0 <= offset <= 10000`. Negative, excessive or non-integer values return 400. Parent statuses: `ACTIVE`, `UNKNOWN`, `FILLED`, `CANCELED`, `REJECTED`, `COMPLETED`. Unknown statuses return 400. A full page may be followed by an empty page; no expensive total-count query is offered. Offset paging is intended for small static demo sets and can shift during concurrent additions.

List envelope:

```json
{"items": [], "limit": 20, "offset": 0}
```

Example parent summary (processing timestamp/version are illustrative):

```json
{
  "root_order_id": "DEMO-1001",
  "status": "FILLED",
  "quantity": 100,
  "filled_quantity": 100,
  "child_count": 2,
  "version": 15,
  "business_updated_at": "2026-01-15T14:30:14Z",
  "persisted_at": "2026-01-15T14:31:00Z",
  "warnings": []
}
```

Children contain `root_order_id`, `order_id`, `status`, `quantity`, `filled_quantity`, decimal `price`, `pending_cancel`, `pending_replace`, and `warnings`. Quantity/price are null if preceding terms are missing. Pending requests are separate from execution status; rejection does not invent a terminal child state.

Timeline entries contain `root_order_id`, `event_id`, `order_id`, `source`, `event_type`, `occurred_at`, `source_sequence`, and `payload` (the full validated event object using the producer's camelCase wire fields). Duplicate event IDs appear only once. Time order is a visual aid, not a causal assertion.

Validation errors and not-found responses use Spring Problem Details, for example:

```json
{"type":"about:blank","title":"Bad Request","status":400,"detail":"limit: 1..100; offset: 0..10000"}
```

Database read failures return 503 with `Database temporarily unavailable`, without SQL or connection credentials. Health may report pipeline starting/rebalancing/retrying as 503. There is no write endpoint or user authentication; loopback-only binding is the intended local demo configuration.
