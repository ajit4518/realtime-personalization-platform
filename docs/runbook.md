# Runbook

On-call procedures for every alert this platform raises. Each section states what the alert means, what to check first, and how to mitigate before diagnosing.

---

## QoE regression

**Alert:** `qoe_regression` on the `qoe.alerts` topic. Raised by `streaming/jobs/qoe_anomaly.py` when a CDN point of presence shows rebuffer ratio at least 3 standard deviations above its own recent baseline, above an absolute floor of 5%, across at least 25 sessions.

**What it means.** Viewers served by one edge location are experiencing materially worse playback than that location's own recent norm. It is scoped, not global — the detector compares each POP against itself precisely so that structurally slower regions do not alert constantly.

**First checks, in order:**

1. Read the alert payload. `sessions_affected` tells you the blast radius, `z_score` tells you the severity, `error_rate` distinguishes a delivery problem from a capacity problem.
2. Check whether other POPs on the same CDN provider are also degraded. Query `mart_qoe_daily` filtered to the last hour grouped by provider. Multiple POPs on one provider means a provider incident; one POP means a local one.
3. Check `p95_startup_ms` in the same window. Elevated startup with normal rebuffering points at manifest or DRM, not bandwidth.
4. Check whether a client release went out recently. `stg_playback_events` carries `app_version`; a regression confined to one version is a client bug, not a network one.

**Mitigation before diagnosis.** If one POP on a multi-provider region is affected, shift traffic away at the CDN configuration level. Restoring viewers first and diagnosing second is correct here — the failure is already costing subscriptions.

**False positive pattern.** A large content release drives a traffic spike that briefly outpaces edge capacity, resolving on its own within a few minutes. Check `sessions` against the same hour on prior days before escalating to the provider.

---

## Feature freshness breach

**Alert:** Redis feature age exceeding the 5-second SLO at p99, from the `flink_features_emitted` metric against Redis write timestamps.

**What it means.** The streaming path is falling behind. Recommendations are still being served — the API falls back to offline features — but personalisation is degraded to yesterday's picture.

**First checks:**

1. Flink UI, checkpoint duration and back-pressure. Sustained back-pressure on the sessionize operator means the job cannot keep up with input.
2. Kafka consumer lag on group `flink-session-features`. Growing lag confirms it.
3. Redis write errors: the `redis_write_errors` counter in the job metrics. A failing sink drops batches silently by design, so this is where that shows up.
4. RocksDB state size. Unbounded growth means a TTL is not being applied and state has outgrown the task manager's memory.

**Mitigation.** Increase parallelism (`FLINK_PARALLELISM`) and restart from the latest checkpoint. If state size is the cause, that is not fixable at runtime — the fallback path is doing its job, and the fix belongs in a job change.

**Do not** restart the job without taking a savepoint first. Session state that has not checkpointed will be lost and in-flight sessions will restart their accumulation from zero.

---

## CDC replication lag

**Alert:** Debezium `MilliSecondsBehindSource` above 60 seconds, or the RDS replication slot's retained WAL above 8 GB.

**What it means.** Change capture is behind. The warehouse will be building from stale data, and — far more seriously — **retained WAL is growing on the production database**.

**This one escalates fast.** If the slot's retained WAL approaches `max_slot_wal_keep_size` (32 GB), Postgres drops the slot, and recovering requires a fresh snapshot of every replicated table. If the parameter were unset, the volume would fill and the OLTP database would go down. Treat sustained growth as a production database incident, not a pipeline one.

**First checks:**

1. Connect status: `curl localhost:8083/connectors/streaming-postgres-source/status`. A failed task shows its stack trace here.
2. The dead letter topic `dlq.cdc.postgres`. A poison record with `errors.tolerance: all` routes here rather than stopping the connector.
3. Whether a bulk update ran against the OLTP database. A million-row migration generates a WAL burst that legitimately takes time to drain.
4. Slot state in Postgres:
   ```sql
   select slot_name, active,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as retained
   from pg_replication_slots;
   ```

**Mitigation.** Restart a failed task with `POST /connectors/streaming-postgres-source/tasks/0/restart`. If the connector cannot be recovered quickly and WAL is approaching the cap, **drop the replication slot deliberately** rather than letting it take down the database, and plan a re-snapshot. Losing a day of CDC is recoverable; losing the OLTP database is not.

---

## Recommendation latency breach

**Alert:** `recommendation_latency_seconds` p99 above 50 ms for 5 minutes.

**First checks:** the `recommendation_stage_seconds` histogram breaks the request down by stage, which usually identifies the cause immediately.

| Stage elevated | Likely cause |
|---|---|
| `features` | Redis slow or the circuit breaker is flapping. Check ElastiCache CPU and evictions. |
| `candidates` | FAISS index grew after a rebuild; check `efSearch` against index size. |
| `ranking` | Candidate pool larger than expected, or a new model with far more trees. |

**Mitigation.** Scale out — the HPA should already be doing this, so check whether it has hit `maxReplicas`. If the cause is a model change, roll back to the previous artefact: `helm -n serving rollback recommendations`.

**Check the `served_from` label distribution.** A spike in `popularity_fallback` means the feature store is failing and the fallback path is carrying the traffic. Latency may look fine while relevance is quietly degraded, which no latency alert would catch.

---

## Model promotion blocked

**Not an alert, but a recurring question.** `model_retraining` reports `skip_promotion` when the candidate does not beat the incumbent by at least 0.002 NDCG@10, or when the cold-start segment regressed by more than 5%.

This is working correctly. A retrain that completes is not a retrain that helps. Read the `promotion_decision` tag on the MLflow run for the specific reason before investigating.

Repeated skips over several weeks are worth investigating, though — usually it means the feature set has stopped capturing something that has changed about viewer behaviour, not that the gate is too strict.
