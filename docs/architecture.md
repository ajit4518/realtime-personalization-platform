# Architecture

How data moves through the platform, where state lives, and what happens when
each component fails.

---

## Data flow

There are two paths from the same events, with different latency and
correctness properties.

### The fast path (seconds)

```
player SDK ──► playback.events ──► Flink ──► Redis ──► recommendation API
   emit          Kafka topic      sessionize   feature    ranked response
                                              vector
```

An event emitted by a player is visible to the ranker in about 1.4 seconds at
the median. Flink maintains per-session and per-profile state, and writes a
compact feature vector keyed by profile.

This path is **eventually consistent and lossy under stress by design**. If
Redis is unavailable the sink drops batches rather than stalling; if a feature
key is missing the API falls back to nightly features. Degraded personalisation
is an acceptable outcome, a stalled pipeline is not.

### The slow path (hours)

```
playback.events ──► Kafka Connect ──► S3 (Parquet) ──► Redshift ──► dbt ──► marts
                                                                        └─► features
Postgres WAL ──► Debezium ──► cdc.* topics ──┘
```

Everything lands in S3 partitioned by event date and hour. dbt builds staging,
marts, and the point-in-time feature tables that training reads.

This path is **exactly-once and complete**. It sees late arrivals the fast path
missed, and it is the source of truth for anything a human will make a decision
from.

The two paths compute the same feature definitions and are compared daily. See
[ADR 0002](adr/0002-dual-path-features-with-parity-testing.md).

---

## Where state lives

| State | Store | Lifetime | Lost on failure? |
|---|---|---|---|
| Session accumulation | Flink RocksDB | 60 min TTL | Recovered from last checkpoint (30s) |
| Profile rollups | Flink RocksDB | 30 day TTL | Recovered from checkpoint |
| Online features | Redis | 24 h TTL | Yes — falls back to offline features |
| Offline features | Redshift | Retained | No |
| Raw events | S3 | 2 years, tiered | No |
| Kafka topics | MSK | 7 days | No, within the window |
| Consumer offsets | Kafka | Retained | No |
| Model artefacts | S3, versioned | Retained | No |

Two deliberate properties. **Every piece of streaming state has a TTL** —
unbounded state growth is the most common way a long-running Flink job
eventually dies, and it always happens at the worst moment. **Nothing
irreplaceable lives only in Redis** — it is a cache with an eviction policy,
and treating it as a database would make a cache eviction a data loss.

---

## Failure modes

Written as "what happens" rather than "what should happen", because the
distinction is the whole point of designing for failure.

### Redis unavailable

The circuit breaker in `serving/app/features.py` opens after 10 consecutive
failures and short-circuits for 15 seconds. Requests are served from the
popularity fallback. Latency stays within SLO; relevance degrades.

The Flink sink logs and drops feature batches. When Redis returns, the next
event for each session rewrites its key, so recovery is automatic and needs no
backfill.

**Blast radius:** relevance quality. Not availability.

### Flink job fails

Kubernetes restarts it; the job resumes from the last checkpoint (at most 30
seconds of reprocessing). Kafka offsets are part of the checkpoint, so no events
are missed or double-counted.

Redis keeps serving the last-written features, which grow stale at the rate the
outage lasts. Beyond the 24-hour TTL, keys expire and every profile falls back
to offline features.

**Blast radius:** feature staleness proportional to downtime.

### Kafka unavailable

Producers buffer and then block. The player gateway sheds telemetry rather than
failing user playback — losing analytics is preferable to a viewer's stream
stopping.

Flink stalls and resumes from committed offsets when brokers return. Nothing
within the 7-day retention window is lost.

**Blast radius:** telemetry gaps only, if the outage exceeds producer buffers.

### CDC replication lag

The most dangerous failure in the platform, because the damage lands on the
production OLTP database rather than on the pipeline.

An unconsumed replication slot retains WAL indefinitely. Left alone, the RDS
volume fills and the database stops accepting writes. `max_slot_wal_keep_size`
caps retention at 32 GB, so Postgres drops the slot instead — the pipeline
breaks and requires a re-snapshot, but the database survives.

That is the right trade and it is a deliberate one. See
[runbook](runbook.md#cdc-replication-lag).

**Blast radius:** warehouse freshness, and potentially the OLTP database if the
guardrail were removed.

### Warehouse build fails

Marts hold yesterday's data. The offline feature publish does not run, so Redis
keeps the previous day's fallback features (TTL is 30 hours, deliberately
longer than a day, so one failed run degrades rather than empties).

Training is blocked by the `ExternalTaskSensor` and skips the week rather than
training on partial data.

**Blast radius:** reporting freshness. Serving is unaffected.

### Bad model promoted

Caught at three layers before it can serve broadly:

1. The evaluation gate blocks promotion unless NDCG@10 improves and the
   cold-start segment has not regressed.
2. The smoke test scores fixed profiles and fails on degenerate output — for
   instance every score identical because a feature column went null upstream,
   which metrics alone would not catch.
3. The canary takes 5% of live traffic for ten minutes with error-rate
   monitoring before the full rollout.

Rollback is `helm rollback`, or re-promoting the previous MLflow version, which
is archived rather than deleted for exactly this reason.

---

## Scaling characteristics

**What scales horizontally without thought:** the recommendation API (stateless,
HPA on requests-per-pod), Kafka Connect sink tasks, Flink parallelism up to the
partition count.

**What needs planning:** Flink parallelism cannot exceed the Kafka partition
count, so partitions are the real ceiling — `playback.events` is provisioned at
12 partitions against a current need of 4. Redis is sharded three ways in
production; resharding is an online operation but not a free one.

**What does not scale by adding machines:** the point-in-time feature model is
quadratic in (profiles × days) if rebuilt from scratch. It is incremental, and a
full rebuild is a planned operation rather than something to run casually.

**The current bottleneck** at 40k events/sec is the Flink sessionization
operator's RocksDB write path, not Kafka and not the API. The next step would be
to increase parallelism and partition count together.

---

## Security

- No component is publicly reachable. The ALB is internal; EKS has no public
  endpoint in production.
- Every data store is encrypted at rest with a customer-managed KMS key and in
  transit with TLS.
- MSK uses IAM authentication, so there are no Kafka passwords to rotate.
- The RDS master password is managed by Secrets Manager and rotates
  automatically; it appears in neither Terraform state nor code.
- Pods assume IAM roles via IRSA. No AWS credentials exist inside the cluster.
- Containers run as non-root with a read-only root filesystem and all
  capabilities dropped.
- The OLTP security group has no egress rule at all — the database has no
  reason to initiate outbound connections.
- `email_hash` rather than raw email in the OLTP schema, so no direct
  identifier propagates into the lake through CDC.
