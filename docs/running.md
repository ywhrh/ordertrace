# Running, demonstrating and replaying OrderTrace

[Back to the project overview](../README.md)

## Quick start

Prerequisites: Java **21**, Maven **3.9+**, Docker with Compose v2, available local ports **5432**, **9092**, **8080**. First build/pull needs internet. Start Docker Desktop first on macOS/Windows.

```bash
docker compose up -d --wait
mvn clean verify
java -jar target/ordertrace-1.0.0.jar
```

In another terminal, from the repository root:

```bash
bash scripts/demo.sh
curl -fsS http://localhost:8080/api/health
curl -fsS 'http://localhost:8080/api/parents?limit=20&offset=0'
```

Open [http://localhost:8080](http://localhost:8080). Processing is asynchronous: refresh until the expected summaries appear. The page displays pipeline health, query time, event time and persistence time. Select a parent to inspect its children and expandable raw timeline payloads. Filters and all lists have bounded pagination.

Default local demo database: `ordertrace`, user `ordertrace`, password `ordertrace_demo`. This is an explicit disposable local credential, not a secret. Services bind to loopback. To override settings, copy `.env.example` to `.env` and export it before launching Java (`set -a; source .env; set +a` in bash). Compose reads `.env` itself; Java reads environment variables, not `.env` files. Changing the PostgreSQL image environment password does not alter an already initialized database volume.

## Five-minute demonstration

1. Publish `data/demo.ndjson` with `bash scripts/demo.sh`. Three producer instances use client IDs `simulator-CG`, `simulator-SCEPTER`, `simulator-EG`; one command coordinates arrival order. There are 29 records and 27 unique events.
2. Select **DEMO-1001**: two children, total quantity/fills 100, parent FILLED from Scepter. Child A partially fills then fully fills. Child B accepts a replace, rejects a later replace and cancel, then fills. Arrival order deliberately includes a parent final before creation and fills before ACK.
3. Select **DEMO-1002**: 25/100 filled, child and parent CANCELED. Successful cancel preserves fills.
4. Select **DEMO-1003**: orphan fill, UNKNOWN parent, missing creation/ACK/sequence warnings.
5. Select **DEMO-1004**: child FILLED 50/50, parent still ACTIVE because Scepter has not published a final. Downstream does not infer parent completion.
6. Run `bash scripts/demo.sh` again. Event IDs and execution IDs are stable; totals and distinct timelines do not grow.
7. Stop Java with Ctrl-C, restart the same command and refresh. Same application ID/state/changelog/consumer offsets provide **recovery**, not a new full replay.
8. With publishers stopped and normal results caught up, run `bash scripts/replay.sh`. It prints the retained raw offset range, drains an isolated replay, and checks PostgreSQL business equality.

| Parent | Authoritative state | Quantity | Filled | Children | Distinct events |
|---|---|---:|---:|---:|---:|
| DEMO-1001 | FILLED | 100 | 100 | 2 | 15 |
| DEMO-1002 | CANCELED | 100 | 25 | 1 | 7 |
| DEMO-1003 | UNKNOWN | null | 10 | 1 | 1 |
| DEMO-1004 | ACTIVE | 50 | 50 | 1 | 4 |

The corrected Scepter event contract uses fresh v2 raw/result topics, application ID and default PostgreSQL schema `ordertrace_v2`. Earlier demo topics/schemas are preserved but are not read by this version. If you exported old overrides from `.env`, update them from `.env.example` before starting. Publish the corrected sample once to populate the new namespace.

## Recovery versus full replay

**Recovery:** restart with the same `OT_APPLICATION_ID` (default `ordertrace-live-v2`), result topic and PostgreSQL schema. Kafka Streams restores local/changelog state and resumes committed input offsets; the DB writer resumes its consumer group. Preserve Docker volumes and the `.runtime/streams` directory. The changelog can restore state if local state is lost, provided the Kafka internal topics remain intact.

**Replay:** the command requires a fresh namespace beginning `replay_`. It uses a new Streams application ID, a separate result topic, a new writer group and a separate PostgreSQL schema. It refuses existing namespaces, never drops/truncates the normal schema, and does not switch the UI to replay data.

```bash
bash scripts/replay.sh replay_tonight
# Or use an automatically generated timestamp namespace:
bash scripts/replay.sh
# Re-run a business comparison without starting processing:
java -jar target/ordertrace-1.0.0.jar compare replay_tonight
```

Keep publishers stopped and wait for the normal app to catch up before replay. The command captures earliest/latest raw offsets, starts at earliest retained offsets, checks that the raw range stays unchanged, and waits for Streams and writer consumer offsets to drain. It exits nonzero after a three-minute timeout or mismatch. A failed replay leaves its isolated evidence for inspection; choose a fresh namespace to retry. Namespace creation is a single-operator procedure, not a concurrent distributed lock.

Comparison includes every parent business field, every child and every timeline payload; it excludes `persisted_at` and technical aggregate version. If raw history has expired, reconstruction may be incomplete and comparison is expected to fail. Seven-day retention is segment-based and disk-dependent, not a precise archival guarantee. Retained Streams state is not a substitute for missing raw replay history.

Dedup evidence has no TTL in this MVP and is bounded to **500 unique events per root**; overall state grows with root count. Exceeding the per-root limit stops processing visibly. Do not reset an application ID into an existing result/schema namespace or repartition the populated input topic. Use a new isolated replay namespace for a rebuild.

## API examples

```bash
curl -fsS 'http://localhost:8080/api/parents?status=FILLED&limit=20&offset=0'
curl -fsS 'http://localhost:8080/api/parents?rootOrderId=DEMO-1001'
curl -fsS http://localhost:8080/api/parents/DEMO-1001
curl -fsS http://localhost:8080/api/parents/DEMO-1001/children
curl -fsS 'http://localhost:8080/api/parents/DEMO-1001/events?childOrderId=DEMO-1001-B&limit=20'
```

See [API reference](api.md) for response fields, ordering, validation and error semantics.

## Tests

```bash
mvn test                    # reducer + Kafka Streams TopologyTestDriver, no Docker needed
mvn -Pintegration verify    # real Kafka/PostgreSQL via Testcontainers; Docker required
python3 scripts/check-dependencies.py  # runtime OSV + POM license declarations; network required
```

Integration tests use isolated containers and do not reuse the Compose database. They exercise Kafka → Streams → result topic → PostgreSQL → real HTTP API, transactional rollback, duplicate/stale writes, same-ID stream restart, database pause/resume and isolated replay. Docker absence is a test failure, not a silently skipped success. Unit tests alone are not end-to-end evidence. Exact executed results are recorded in [verification](verification.md).

## Operational boundaries and tradeoffs

- A full per-root evidence snapshot is intentionally simple and inspectable. Recomputing and re-sending it costs O(events/root), and duplicate results may still be emitted. DB writes remain idempotent. No throughput or latency benchmark is claimed.
- PostgreSQL outages after startup are retried with bounded connection/socket timeouts; Kafka results buffer the backlog within retention/disk limits. The initial application requires DB availability for migration. Health returns 503 while processing is unhealthy; already persisted read results can still be queried if DB is available.
- Malformed/conflicting input halts Streams rather than silently dropping evidence. Logs and health expose it; no DLQ, automated repair or operator console is built.
- A single broker/replica, one application process, plaintext loopback Kafka and a local DB owner account are demo choices. There is no HA, auth, TLS setup, remote deployment or production hardening.
- No event correction, real exchange request correlation, business-day rollover, guaranteed global ordering or complete financial lifecycle semantics. Rules and missing-information warnings are explicit in [event-model.md](event-model.md).
- `docker compose down` stops local dependencies while preserving volumes. Do **not** use `down -v` if retaining demo/replay history matters. Replay namespaces are retained; this MVP does not automate their deletion.

