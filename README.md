# Kafka DLQ Manager

**Observe, diagnose, replay, and resolve dead-letter messages.**

Spring Kafka makes it easy to *send* failed messages to a dead-letter topic — and then leaves you on your own. Messages pile up in `*.DLT` topics with no way to see why they failed, no way to group failures, and no safe way to replay them after you ship a fix. Teams end up writing throwaway console-consumer scripts at 2 AM.

Kafka DLQ Manager is the missing post-failure lifecycle:

- **Discover** every DLQ topic on the cluster, with depth (noisiest first)
- **Browse** the newest dead letters with full headers and payloads — non-destructively
- **Diagnose** by grouping failures: `SerializationException @ orders → 342 messages`
- **Replay** by coordinates with dry-run, rate limiting, and lineage tracking — never by pasting payloads

## Quickstart (2 minutes)

```bash
git clone <this-repo> && cd kafka-dlq-manager/examples
docker compose up --build
```

This starts Kafka, a demo consumer that deliberately fails on ~30% of orders, and the DLQ manager. Within 30 seconds:

Open **http://localhost:8090** for the web UI: browse dead letters per topic, group failures by exception, and replay selected messages with a dry-run preview before anything is actually produced.

Prefer the terminal:

```bash
# See the DLQ filling up
curl localhost:8090/api/v1/topics

# Newest dead letters, with exception class, origin, delivery attempts
curl localhost:8090/api/v1/topics/orders.DLT/messages?limit=10

# Group failures by exception @ origin
curl localhost:8090/api/v1/topics/orders.DLT/failures

# Dry-run a replay — nothing is produced
curl -X POST localhost:8090/api/v1/replay \
  -H 'Content-Type: application/json' \
  -d '{"dlqTopic":"orders.DLT","messages":[{"partition":0,"offset":0}],"dryRun":true}'

# Execute it for real
curl -X POST localhost:8090/api/v1/replay \
  -H 'Content-Type: application/json' \
  -d '{"dlqTopic":"orders.DLT","messages":[{"partition":0,"offset":0}],"dryRun":false}'
```

## Two pieces, both optional independently

### 1. `dlq-manager-starter` — richer dead letters

Add one dependency to your Spring Kafka consumer services:

```xml
<dependency>
  <groupId>io.kafkadlq</groupId>
  <artifactId>dlq-manager-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Zero code changes. It auto-configures a `DefaultErrorHandler` with an enriched dead-letter recoverer that stamps standardized `x-dlq-*` headers on every dead letter: exception class and stack trace, original topic/partition/offset, consumer group, application name, delivery attempts, failure timestamp. Already have your own error handling? Define your own `DefaultErrorHandler` bean and the starter backs off, or set `dlq-manager.auto-error-handler=false` and just use the recoverer bean.

The header protocol is framework-agnostic (`x-dlq-*`), so Python/Go/Node consumers can adopt it too.

### 2. `dlq-manager-server` — the manager

A single Spring Boot app (one Docker image) pointed at your cluster:

```bash
docker run -p 8090:8090 -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 kafkadlq/dlq-manager-server
```

Works against *any* DLT, even without the starter — you just get degraded diagnostics (no exception grouping) until the headers exist.

## Safety model

- Browsing uses throwaway consumer groups with commits disabled — it can never advance a real consumer group or hide a message
- The API only reads topics matching the configured DLQ patterns — it cannot be used as a generic topic reader
- Replay accepts only coordinates, never payloads; authoritative bytes are re-read from the DLQ
- Replay is non-destructive, dry-run first, rate limited, and stamps lineage headers (`x-dlq-replay-count`) so poison pills are visible

Read [docs/replay-semantics.md](docs/replay-semantics.md) for the full contract, including the at-least-once caveat and ordering guarantees.

## API

| Method | Path | Description |
|---|---|---|
| GET | `/api/v1/topics` | Discovered DLQ topics with partition count and depth |
| GET | `/api/v1/topics/{topic}/messages?limit=50` | Newest messages with headers and diagnostics |
| GET | `/api/v1/topics/{topic}/failures?sampleSize=500` | Failure groups: `exception @ origin → count` |
| POST | `/api/v1/replay` | Replay by coordinates; supports `dryRun`, `targetTopic`, `ratePerSecond` |

Metrics at `/actuator/prometheus`.

## Web UI

`dlq-manager-server` serves a static single-page UI at `/` — no separate build step or Node toolchain, it ships inside the same Docker image. It covers:

- **Browse** — pick a DLQ topic in the sidebar, see the newest messages with headers, diagnostics, and payload (expandable per row)
- **Group** — a Failures tab groups messages by `exception @ origin → count`; clicking a group filters the Messages tab down to matching rows
- **Replay** — select one or more messages and click Replay. The UI always runs a dry run first and shows the resolved target topic (or skip reason) per message; only after reviewing that can you confirm the real replay

## Roadmap

- Avro/Protobuf deserialization via Schema Registry (pluggable SPI)
- Explicit discard workflow with audit trail
- Scheduled auto-replay with backoff
- DLQ depth alerting (Slack/PagerDuty)
- Multi-cluster support

## Building

Requires Java 21 and Maven 3.9+.

```bash
mvn clean verify
```

## License

Apache 2.0
