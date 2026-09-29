# Dependencies and public-source hygiene

Review date: 2026-09-28 (local). This is a basic dependency/license check, not a penetration test or a guarantee that the application is vulnerability-free.

## Fixed versions

Java 21; Spring Boot 3.5.16 BOM; Kafka client/Streams/broker 3.9.2; PostgreSQL container 17.11; Flyway 11.7.2 (BOM); Testcontainers 1.21.4. Maven plugins inherit explicit versions from the pinned Spring Boot parent. Images use exact release tags, not `latest`; tags are not digest locks and base-image contents can change on republish.

The initial runtime OSV query flagged five Maven coordinates. Fixed-version overrides were applied and the resolved graph checked again:

| Component | Final version | Why overridden |
|---|---|---|
| Tomcat embedded modules | 10.1.59 | Published patch beyond the 10.1.58 advisory fix boundary; 10.1.58 itself was not available in Maven Central |
| Jackson BOM | 2.21.6 | Databind advisory fixes |
| Log4j API/bridge | 2.25.5 | MapMessage JSON serialization advisory fix |
| PostgreSQL JDBC | 42.7.12 | Channel-binding downgrade advisory fix |
| LZ4 Java | 1.11.1 | Native range-check/JVM crash advisory fix |

Final scan: **48 runtime Maven coordinates queried; 0 matched OSV advisories**. `scripts/check-dependencies.py` repeats the query over resolved coordinates and saves `target/osv-runtime.json` plus a POM-derived license table. It requires Maven, Python 3, curl and internet, sends only public dependency names/versions, and fails on a query error or matched advisory. OSV coverage can be incomplete or change after this date. Test/build dependencies and container OS packages are not included in this runtime scan.

Sources: [OSV API](https://google.github.io/osv.dev/api/), [Tomcat security](https://tomcat.apache.org/security-10.html), [Kafka CVEs](https://kafka.apache.org/community/cve-list/), [PostgreSQL 17 security](https://www.postgresql.org/support/security/17/), [Spring Boot 3.5.16 release](https://spring.io/blog/2026/06/25/spring-boot-3-5-16-available-now/).

PostgreSQL's official security table motivated the container upgrade from 17.6 to 17.11. The broker/client use Kafka 3.9.2's security-maintained patch release. No claim is made that a Java dependency override changes libraries bundled in the broker container; a full container image scan was not performed.

## Licenses

The application is MIT. See [runtime license declarations](dependency-licenses.md) for the resolved graph. Main libraries are Apache-2.0, MIT or BSD licensed. Some dependencies offer license choices: RocksDB declares Apache-2.0/GPL-2.0; Logback declares EPL-2.0/LGPL-2.1; Jakarta Annotations declares EPL-2.0/GPL with classpath exception. Do not describe the entire binary distribution as MIT-only. Preserve upstream notices/licenses if distributing bundled binaries; dependencies were not modified or vendored. PostgreSQL is under the PostgreSQL License; Docker images also contain separately licensed operating-system packages.

No unknown runtime POM license declarations remained in this check. POM declarations are evidence, not a legal opinion or a complete container notice inventory. Publishing this application's source does not copy third-party source code into it.

## Repository hygiene

- No existing project's source, Git history, credentials or sensitive files were used.
- Synthetic data is deterministic and fictional. Default `ordertrace_demo` is a documented local demo password, not a production credential.
- Loopback-bound ports; no telemetry integration, hosted deployment or remote push.
- `.env`, build output, logs and local Streams state are ignored. Public files contain no personal absolute paths.
- Rerun the dependency check and review the actual staged files immediately before a public push. A dedicated secret scanner and full container scanner were not run.
