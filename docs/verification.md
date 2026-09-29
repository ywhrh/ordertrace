# Verification record

Executed locally on 2026-09-28 with Java 21, Maven 3.9.16 and Docker Desktop (macOS arm64). This is functional evidence, not a performance or production-readiness report.

| Check | Result |
|---|---|
| Java Maven build/package | Passed |
| `LifecycleTest` | 6 tests, 0 failures/errors/skips |
| `OrderTraceIT` against real Kafka 3.9.2 + PostgreSQL 17.11 | 5 tests, 0 failures/errors/skips |
| Compose broker/database health | Both healthy |
| Packaged CLI publisher → Compose Kafka → Streams → result topic → PostgreSQL | Four expected parents, five children, 27 distinct events |
| Browser | Parent table, selection, summary, child list, timeline, child/status filtering and health inspected |
| Isolated CLI replay and PostgreSQL comparison | PASS; business parents/children/event payloads equal |
| Runtime OSV query after patch overrides | 48 coordinates; 0 matched advisories |
| Resolved runtime POM license declarations | 48 inspected; no unknown declarations |

The six unit tests cover real TopologyTestDriver serialization/state processing; repeated events; random arrival permutations; parent final authority; missing predecessors; cancel/replace rejection preserving state; replacement retaining logical IDs; late ACK not reopening terminal state; execution-ID deduplication and conflicting-source/event rejection.

The five integration tests start isolated real containers and a real HTTP server. They cover persisted expected totals, distinct timelines, filtered/paginated API queries and error status codes; equal/stale database writes; transaction rollback after a uniqueness violation; closing and reopening the same Kafka Streams application ID, consuming a new event envelope for a previously seen execution before any history is republished, and then republishing duplicates; pausing and resuming the actual PostgreSQL container while Kafka continues; and a new application/result topic/schema replay compared through PostgreSQL queries.

The delivered scripts were also executed from the final repository directory against Compose. Starting the packaged app from a fresh local state directory with the existing application ID restored processing; repeating the demo and running `replay_final_check` produced a passing PostgreSQL comparison. The normal data remained the four documented synthetic parents.

The first database-pause test exposed an ineffective JDBC URL timeout setting. It was fixed by configuring the connection/socket timeouts as data-source properties; the real pause/resume test then passed. Code inspection also found that a shutdown must finish the current polled writer batch rather than commit positions after breaking out early; the final writer preserves that requirement.

Reproduce with `mvn -Pintegration verify`, then run the README Compose demonstration and `bash scripts/replay.sh` with no publishers running. Reports are generated under `target/surefire-reports` and `target/failsafe-reports`; they are intentionally excluded from Git because they include machine paths and runtime metadata.

Not verified/claimed: performance benchmarks, long-running retention expiry, multi-broker failure or HA, process kill at every transaction boundary, concurrent operators creating replay namespaces, every possible invalid Kafka payload, full container-image CVE inventory, a dedicated secret-scanner pass, or production security. There is no production deployment or remote GitHub publication.
