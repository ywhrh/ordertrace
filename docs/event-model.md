# Event model and deterministic state rules

All orders, prices and executions are synthetic. Quantity is an integer number of units; price is a positive decimal with at most six fractional digits. This demo uses a single unspecified currency and no trading-day rollover.

## Wire contract

The Kafka record key **must equal `rootOrderId`**. The raw topic has three fixed partitions. Do not change its partition count on a populated demo: that can change key placement and break per-root history ownership.

| Field | Meaning |
|---|---|
| `eventId` | Immutable identifier, unique within a root; identical redelivery is ignored |
| `rootOrderId` | Stable parent ID, Kafka partitioning key |
| `orderId` | Logical order actually affected; stable through replace |
| `parentOrderId` | Null for parent; must equal root for a child |
| `source` | `CG`, `SCEPTER`, `EG` |
| `eventType` | Type from the authority table below |
| `occurredAt` | ISO-8601 UTC instant for display, never global ordering authority |
| `sequence` | Positive, contiguous sequence starting at 1 within **(source, orderId)**; must survive publisher restart |
| `quantity`, `price` | Order terms, required on creation and accepted replacement (parent creation requires quantity only) |
| `fillQuantity`, `executionId` | Positive delta and stable execution identifier; FILL only |
| `finalStatus` | Scepter parent final: `FILLED`, `CANCELED`, `REJECTED`, `COMPLETED` |

IDs allow letters, digits, `_`, `.`, `-`; order IDs max 80, event/execution IDs max 100 characters. Quantity/fill delta: 1–1,000,000,000. Numeric prices fit PostgreSQL `numeric(20,6)`.

| Authority | Accepted event types |
|---|---|
| CG | `PARENT_CREATED` |
| Scepter | `CHILD_CREATED`, `PARENT_FINAL` |
| EG | `ACK`, `FILL`, `CANCEL_REQUESTED`, `CANCEL_ACCEPTED`, `CANCEL_REJECTED`, `REPLACE_REQUESTED`, `REPLACE_ACCEPTED`, `REPLACE_REJECTED` |

Requests represent EG's observation of a request forwarded to the exchange. This deliberately avoids ordering a Scepter request against an EG response. It does not model separate customer request delivery or request IDs; only one outstanding request of each kind per child is supported. An upstream relay must not manufacture a new FILL. A non-EG FILL is rejected rather than counted.

## Reducer

The persistent Kafka Streams `order-evidence` key-value store holds a map of unique event IDs for each root, with a Kafka changelog. Each new unique event increments the root's version. Identical duplicates do not. A bounded full-evidence recomputation is used so any permutation of the same valid event set converges. Limits: **500 unique events/root**, not a production throughput design. No event-time window, grace period or TTL silently discards late evidence.

1. Earliest CG creation by CG sequence defines parent quantity. Multiple creations are flagged. Scepter's highest-sequence parent final event controls the authoritative final status. Otherwise parent is `ACTIVE` if created, `UNKNOWN` if not. Even when every known child is FILLED, parent remains ACTIVE without Scepter final.
2. Earliest Scepter child creation provides original quantity/price. EG events are reduced in EG sequence order, independently for each child. Accepted replacement updates terms on that same logical ID. There is no comparison between CG, Scepter and EG sequences.
3. EG fill deltas are summed once per `(root, child, executionId)`. Repeated event IDs are ignored; the same execution under a different event ID is retained in the timeline but is not summed again. Conflicting quantities for the same execution ID stop processing.
4. Child state precedence is FILLED (known quantity reached), CANCELED (accepted cancel), PARTIALLY_FILLED, ACKNOWLEDGED, CREATED, UNKNOWN. This is a deterministic demo rule, not a full exchange state machine. Overfills are preserved and flagged; quantities are not clamped.
5. Requests only set pending flags. Rejections clear the relevant pending flag and preserve execution state and last accepted terms. Accepted cancel clears both pending flags. Accepted replace is ignored with a warning after terminal state or if the proposed quantity is below already observed fills. A fill can still be counted after cancel, representing an in-flight execution fact; filled quantity takes precedence if the total reaches the known order quantity. No bust/correction is supported.
6. Missing creation/ACK/request, sequence gaps and inconsistent totals are visible warnings. Late predecessors remove resolved warnings on recomputation. Without known quantity, fills remain PARTIALLY_FILLED with missing-information warnings. `AWAITING_SCEPTER_FINAL` is intentional pending authority, not proof of lost data.
7. `business_updated_at` is maximum observed event time, used only for display. API timeline sorts by event time then event ID; that presentation is not causal order. `persisted_at` separately indicates database write time.

Malformed JSON, wrong Kafka key, wrong authority, conflicting event ID/sequence/execution, or the per-root limit stops the stream client; `/api/health` becomes 503. There is no silent skip and no automatic dead-letter repair UI. Fix the synthetic input and rebuild into a new isolated namespace. Do not change a known event's payload under its ID.

## Storage and consistency

Flyway `V1__read_model.sql` creates parent summaries, child state and raw event payloads. Keys and indexes support root/status filtering and ordered child timelines. `(root,eventId)` and `(root,source,orderId,sequence)` are unique. Each complete snapshot is written in one PostgreSQL transaction.

`INSERT ... ON CONFLICT ... WHERE parents.version < excluded.version` locks and gates the root update. Child upserts and timeline inserts occur only when that gate advances. A failed write rolls everything back. Equal/older versions are no-ops, so retry after a successful DB commit but failed Kafka offset commit is safe. This assumes one Streams application owns each result/schema namespace; versions from unrelated applications are not comparable.

The writer disables auto-commit, reads committed Kafka results and commits each offset **after** the PostgreSQL transaction returns. On failure it rewinds every partition in the polled batch, retries, and accepts duplicate work. A result topic buffers a temporary database outage only within its configured retention and disk capacity. Kafka Streams `exactly_once_v2` covers state, offsets and Kafka output, **not an atomic transaction with PostgreSQL**.

Each REST request reads PostgreSQL. Individual rows/snapshot writes are atomic; several separate HTTP calls can observe successive versions while events are arriving. Pause publishers for a stable demonstration or comparison.
