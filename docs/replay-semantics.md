# Replay semantics

Replay is the most dangerous thing this tool does. This document is the contract.

## What replay is

Replay republishes the exact bytes of a dead-lettered record (key + value + headers) to a target topic. The target is resolved in priority order:

1. `targetTopic` supplied explicitly in the request
2. The `x-dlq-original-topic` header stamped by the starter
3. Naming convention: stripping a recognized DLQ suffix (`.DLT`, `-dlq`, `-dlt`)

If none resolve, the message is **skipped**, never guessed.

## Safety invariants

**Authoritative bytes.** The replay API accepts only coordinates (`partition`, `offset`) — never payloads. The server re-reads the record from the DLQ before producing. What failed is exactly what is replayed; there is no path for a client to inject or tamper with a payload through this API.

**Non-destructive.** Replay never commits offsets or deletes anything on the DLQ topic. A replayed message remains on the DLQ until retention expires it or an operator explicitly discards it (discard is a separate roadmap feature with its own audit trail). This means replay is always recoverable: if a replay goes wrong, the source of truth is untouched.

**Dry run.** `dryRun: true` performs full target resolution for every message and returns exactly what would happen (`WOULD_REPLAY` / `SKIPPED` per item) without producing a single record. UIs should default to a dry run and require a second confirmation to execute.

**Rate limiting.** Replays are throttled (default 50 records/second, configurable per request). The failure that filled the DLQ often means the downstream consumer or its dependencies were unhealthy; dumping ten thousand messages back at full speed can re-trigger the outage. Rate limiting turns replay into a controlled drip.

**Idempotent producer, at-least-once semantics.** The replay producer uses `acks=all` and `enable.idempotence=true`, which prevents broker-side duplicates from producer retries. However, replay is still an *at-least-once* operation end to end: the consuming application may see the same logical message twice (once from the original failed delivery if it partially processed, once from replay). **Consumers must be idempotent for replay to be safe.** This is not a limitation of this tool; it is a property of Kafka itself.

**Replay lineage.** Every replayed record is stamped with:

- `x-dlq-replayed-at` — epoch millis of the replay
- `x-dlq-replay-count` — incremented on each replay

If a message fails again after replay, the next dead letter carries its lineage. A rising replay count on the same key is a signal the failure is deterministic (poison pill) rather than transient — replaying it a fourth time will not help; fix the consumer or discard the message.

## Ordering caveat

Replayed records are appended at the head of the target topic like any new record. Original ordering relative to messages produced after the failure is **not** restored, though key-based partition affinity is preserved (the original key is kept, so a keyed record lands on the same partition as its siblings). Applications relying on strict per-key ordering should replay only when they can tolerate a late-arriving record, or drain the DLQ oldest-first.

## Poison-pill guidance

A poison pill (deterministically failing message, e.g. malformed payload, schema mismatch) will bounce: original topic → consumer fails → DLQ → replay → consumer fails → DLQ. The replay count makes the loop visible. The correct actions are, in order of preference:

1. Fix the consumer, then replay
2. Transform-and-replay to a repair topic (roadmap)
3. Explicitly discard with a recorded reason (roadmap)
