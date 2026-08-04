# ADR 0001: Flink over Spark Structured Streaming

**Status:** Accepted
**Date:** 2026-07-14

## Context

The platform needs a stream processor for two jobs:

1. Sessionize player telemetry and publish feature vectors that the recommendation API reads on every request. The SLO is that a viewer's action is reflected in their features within 5 seconds.
2. Detect quality-of-experience regressions per CDN edge location, with detection latency under a minute.

My working background is heavier in Spark, and Spark Structured Streaming would be the lower-risk choice on familiarity alone. It deserved a real evaluation rather than a default.

## Decision

Apache Flink, running on Kubernetes.

## Reasoning

**Micro-batch is a floor on latency.** Spark Structured Streaming processes in batches at an interval, even in continuous mode. The batch interval is a latency you cannot get under, and at the intervals that keep overhead reasonable (1–2 seconds) it consumes most of a 5-second budget before any actual work happens. Flink processes per record.

**The feature vector must update mid-session, not at session close.** This is the decisive point. Spark's session windows fire when the session ends. That is far too late — the whole purpose is to personalise the *next* thing the viewer sees, while they are still watching. Flink's `KeyedProcessFunction` with event-time timers emits on every event and uses the timer only to close out sessions the client abandoned without a terminal event.

**Late events need first-class handling.** Mobile clients background, lose connectivity, and flush telemetry minutes later. Roughly 3% of production events arrive out of order. Flink's watermarks with bounded out-of-orderness make the correctness-versus-latency trade an explicit parameter. Spark's watermarking is real but coarser, and interacts with the batch boundary in ways that are harder to reason about.

**Keyed state at this size needs RocksDB with TTL.** Per-profile rolling state across tens of millions of profiles does not fit in heap. Flink's RocksDB backend with incremental checkpointing and per-descriptor state TTL handles it directly. Spark's state store has improved but offers less control over expiry, and unbounded state is the most common way a long-running streaming job eventually falls over.

## Alternatives considered

**Spark Structured Streaming.** Rejected for the latency floor and the session-window semantics. It remains the right answer for the batch-shaped work in this platform, which is why the warehouse path is dbt and not Flink.

**Kafka Streams.** Genuinely good at exactly this shape of problem and simpler to operate, with no separate cluster. Rejected because it is JVM-only and the rest of the platform's data code is Python; a Java service in an otherwise Python codebase raises the cost of every future change more than the operational saving is worth.

**ksqlDB.** Fast to build the aggregations in. Rejected because the anomaly detector needs stateful custom logic (EWMA baselines with alert cooldown windows) that would end up as a user-defined function, at which point the declarative benefit is gone and only the constraints remain.

## Consequences

**Accepted costs.** One more runtime to operate and understand. PyFlink is less mature than the JVM API — the custom Redis sink in `streaming/jobs/redis_sink.py` exists partly because the Python connector ecosystem is thinner. Checkpoint tuning is a real operational surface.

**Mitigations.** Every operator has a pinned `uid()` so state survives job upgrades. Every state descriptor has an explicit TTL. Unaligned checkpoints are enabled, which keeps checkpoint duration bounded under back-pressure.

**Revisit if** the latency requirement relaxes past a few seconds, or if the team composition makes a second JVM-adjacent runtime a maintenance problem. Neither would be a small migration, but both would be a legitimate reason to reopen this.
