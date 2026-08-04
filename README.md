# Real-Time Personalization Platform

A production-shaped data and machine learning platform for a streaming service. It ingests player telemetry and database changes, maintains a real-time picture of every viewing session, serves ranked recommendations in under 50 ms at the 99th percentile, and detects playback quality regressions within about 30 seconds of onset.

Everything runs locally with one command. Everything deploys to AWS with Terraform and Helm.

```bash
make demo
curl 'http://localhost:8000/recommendations?profile_id=101&region_code=us-east'
```

---

## The problem this solves

A streaming service loses subscribers for two reasons a data team can actually fix.

**The home screen shows the wrong things.** Recommendations built from yesterday's batch job do not know that someone spent the last twenty minutes watching documentaries. The signal with the most predictive power is the freshest one, and a nightly pipeline throws it away by construction.

**Playback quality degrades before anyone notices.** Streaming failures are almost never global. They are scoped to one CDN edge location, one ISP, one client version. A daily dashboard averages that away, and by the time it surfaces the affected viewers have already left. Cancellation rates in a region with elevated rebuffering run measurably higher than the baseline, and the window to intervene is minutes.

Both problems need the same thing: a pipeline where an event that happens now changes a decision made seconds later, without giving up the correctness and governance that analytical work requires.

## What it does

```
                          ┌──────────────────────────────────────────┐
   Player SDKs ──────────►│  Kafka (MSK)                             │
   ~40k events/sec        │  playback.events · cdc.* · qoe.*         │
                          └───────┬──────────────────────┬───────────┘
   PostgreSQL (RDS)               │                      │
   OLTP system of record          │                      │
        │                         ▼                      ▼
        │  Debezium CDC   ┌───────────────┐     ┌──────────────────┐
        └────────────────►│  Flink        │     │  Kafka Connect   │
                          │  sessionize   │     │  S3 sink         │
                          │  QoE detect   │     └────────┬─────────┘
                          └───┬───────┬───┘              │
                              │       │                  ▼
                 ┌────────────┘       └──────┐    ┌─────────────┐
                 ▼                           ▼    │  S3 lake    │
        ┌─────────────────┐        ┌──────────────┴──┐ Parquet   │
        │  Redis          │        │  qoe.alerts     │           │
        │  online feature │        │  → PagerDuty    │           │
        │  store          │        └─────────────────┘           │
        └────────┬────────┘                                      ▼
                 │                                    ┌────────────────────┐
                 │                                    │  Airflow → dbt     │
                 │                                    │  Redshift          │
                 │                                    │  point-in-time     │
                 │                                    │  features          │
                 │                                    └─────────┬──────────┘
                 │                                              │
                 ▼                                              ▼
        ┌────────────────────────────┐              ┌────────────────────┐
        │  Recommendation API        │◄─────────────│  LightGBM ranker   │
        │  FastAPI on EKS            │   artefact   │  weekly retrain    │
        │  p99 < 50 ms               │              │  promotion gate    │
        └────────────────────────────┘              └────────────────────┘
```

Two paths, one set of definitions. The streaming path computes features in Flink and writes them to Redis for serving. The batch path computes the same features in dbt for training. A daily parity test compares them, because the two silently drifting apart is the single most expensive failure mode in a system like this — nothing breaks, no alert fires, and model quality decays for weeks before anyone connects the dots.

## Measured behaviour

| Metric | Result | How it is measured |
|---|---|---|
| Recommendation latency, p99 | 44 ms | k6 load test, 2,000 rps sustained, `load-tests/recommendations.js` |
| Recommendation latency, p50 | 11 ms | same |
| Event to feature visible in Redis, p99 | 3.8 s | timestamp delta, player emit to Redis write |
| QoE regression detection | 34 s median | injected fault via `make simulate-incident` |
| Sustained event throughput | ~40k events/sec | 12 partitions, 4 Flink task managers |
| Offline-to-online AUC gap | 0.01 | was 0.09 before point-in-time features |

The last row is the one worth explaining, and it is covered under [Point-in-time correctness](#point-in-time-correctness-the-part-that-matters-most).

---

## Technology choices

Each of these was a decision with a real alternative. The reasoning is what matters; the tool is downstream of it.

### PostgreSQL on RDS — the system of record

The OLTP database holds subscribers, catalogue, and subscription state. It is on RDS rather than self-managed because managed backups, multi-AZ failover, and automated minor version patching are not problems worth solving again.

What made Postgres specifically the right choice here is **logical replication**. Postgres exposes its write-ahead log through a replication slot, which means change data capture reads committed transactions in commit order with no polling, no `updated_at` scanning, and no load on the query path. The alternative — periodically querying for rows changed since a watermark — misses deletes entirely, misses updates that revert within the polling interval, and adds read load to the database you are trying not to disturb.

The parameter group in `infra/terraform/main.tf` sets `rds.logical_replication`, and critically `max_slot_wal_keep_size`. An abandoned replication slot holds WAL indefinitely and eventually fills the volume, taking down the production database. That is the most common way a CDC pipeline kills its own source, and it is a one-line prevention.

### Debezium — change data capture

Debezium turns the replication stream into Kafka topics. Chosen over writing a bespoke consumer because it handles the parts that are tedious and easy to get wrong: initial snapshot consistency, schema evolution, restart from the exact log position, and the tombstone semantics that make deletes representable in a log-compacted topic.

The configuration in `ingestion/debezium/postgres-source.json` documents each non-default setting inline. Two worth calling out here:

- `heartbeat.interval.ms` is set because a slot on a low-traffic table otherwise never advances its confirmed position, and the WAL grows without bound.
- `publication.autocreate.mode: disabled`. The publication is owned by a migration, not by the connector, so a connector restart can never silently widen what gets replicated out of the production database.

### Kafka (MSK) — the transport

Every component reads from Kafka rather than calling each other. That decoupling is the reason the streaming layer can be redeployed without the API noticing, and the reason a slow S3 sink cannot apply back-pressure to the player telemetry gateway.

Retention is seven days, which is a deliberate operational choice: it is long enough to replay a full weekend after fixing a bug in a Flink job, without paying to store what the S3 lake already holds permanently.

`min.insync.replicas=2` paired with `acks=all` in the producers is the combination that actually guarantees durability. Either alone does not — `acks=all` against an in-sync replica set of one acknowledges a write that a single broker failure loses.

### Apache Flink — stream processing

**This is the choice most worth defending, because Spark Structured Streaming is the more common answer and I have shipped more of it.**

Spark Structured Streaming is micro-batch. Even in continuous mode, the operational model is batches of records at an interval. For a pipeline whose requirement is "the feature reflects what the viewer did seconds ago", the batch interval is a floor on latency you cannot get under.

Flink processes per event, with genuine event-time semantics and watermarks. Three specific things this platform needs that follow from that:

1. **Incremental emission mid-session.** The feature vector updates while a viewing session is still open. A Spark session window fires at session close, which is far too late to personalise the next row the viewer sees. `session_features.py` uses a `KeyedProcessFunction` with an event-time timer, emitting on every event and using the timer only to close abandoned sessions.

2. **Correct handling of late mobile events.** Mobile clients background, lose connectivity, and flush telemetry minutes later. Flink's watermarks with bounded out-of-orderness handle that explicitly, and the bound is a knob (`BOUNDED_LATENESS_MS = 2000`) that trades correctness against latency in a way that can be reasoned about rather than discovered.

3. **Large keyed state that survives restarts.** Per-profile rolling state across tens of millions of profiles lives in RocksDB with incremental checkpoints and TTL-based expiry. Unbounded state is how streaming jobs die at three in the morning, so every state descriptor in this codebase has an explicit TTL.

The honest tradeoff: this is one more runtime to operate, and the Python API is less mature than the JVM one. For a pipeline with a five-second SLO it is worth it. For a pipeline that feeds a dashboard refreshed hourly it would not be, and I would have used Spark.

### Redis (ElastiCache) — the online feature store

A feature read sits on the critical path of every recommendation request with an 8 ms budget. That rules out anything that touches disk on the read path.

Redis is configured with `allkeys-lru` eviction rather than the default `noeviction`, which is a deliberate statement about what this store is: a cache, not a database. Under memory pressure it should drop the least recently used key, not start rejecting writes — because rejecting writes would back up the Flink sink and stall the entire pipeline over a cache running out of room.

The client in `serving/app/features.py` wraps reads in a circuit breaker. Without one, a Redis outage means every request waits its full timeout before falling back, which at a few thousand requests per second queues faster than it drains and takes down the API through a failure it was supposed to tolerate. With one, the failure becomes an instant fallback to offline features and the home screen stays populated.

### dbt on Redshift Serverless — the warehouse

dbt provides the three things that make analytical SQL maintainable: dependency resolution from `ref()` rather than a hand-maintained DAG, tests that live next to the models they test, and version control over transformation logic that would otherwise be views nobody can diff.

The features used here beyond the basics:

- **Model contracts** (`models/features/_features.yml`) enforce column names and types at build time. A breaking schema change fails the build rather than silently feeding nulls into a training run.
- **Snapshots** capture subscription tier history. The OLTP table holds only current state, so a downgrade overwrites the previous tier and it is gone — exactly the history churn analysis needs.
- **Slim CI** (`--select state:modified+ --defer`) builds only what changed and what depends on it. On this project that is a 40-minute CI run reduced to about four.

Redshift Serverless rather than provisioned because the warehouse is idle eighteen hours a day and busy for three. Paying for peak capacity around the clock would roughly triple the bill for no benefit.

### Airflow — orchestration

Airflow runs the nightly warehouse build and the weekly retrain. The two DAGs demonstrate the patterns that matter more than the tool:

- `warehouse_elt.py` runs snapshots **before** models, always. A snapshot captures state as of when it runs; if a model depending on subscription history runs first, it reads a day-stale snapshot and the point-in-time features are quietly wrong for the most recent day.
- `model_retraining.py` has a **promotion gate**. Training completing is not the same as training helping. A pipeline that promotes unconditionally will, over enough weeks, walk the production model somewhere nobody chose. The DAG branches on measured NDCG improvement against the incumbent, and checks that the cold-start segment has not regressed — a model can improve on average while collapsing on a minority slice.
- Training runs as a `KubernetesPodOperator` rather than on the worker, so a 24 GB memory request exists for the twenty minutes it is needed instead of being permanently reserved on a worker that idles six days a week.

### FastAPI on EKS — serving

FastAPI for async I/O, because this service spends most of its wall-clock time waiting on Redis rather than burning CPU. Async concurrency means one worker handles hundreds of concurrent requests that are all blocked on the network.

The Kubernetes manifests encode operational lessons rather than defaults:

- **Startup, liveness, and readiness probes are three different things.** The startup probe carries the slow model load. Liveness deliberately does *not* check Redis — a liveness probe that checks dependencies turns a Redis blip into a cascading restart across the whole fleet, strictly worse than the original problem. Readiness does check it, so a degraded pod leaves the load balancer without being killed.
- **`preStop` sleeps for 8 seconds** before shutdown, giving the load balancer time to observe the endpoint removal. Skipping this produces a burst of 502s on every single deploy.
- **HPA scales on requests-per-pod, not only CPU.** CPU reacts late here for the same reason async helps: load can double while the service sits waiting on I/O and CPU barely moves.
- **Memory request equals memory limit** (Guaranteed QoS). The model's resident set is fixed and predictable; being evicted under node pressure would be gratuitous.

### LightGBM — ranking

Gradient-boosted trees, not a neural network. On tabular behavioural features at this scale, GBDTs match or beat deep models while training in minutes on CPU and predicting a 400-row batch in 19 ms. A neural ranker would need GPU serving infrastructure to hit the same latency, for no measured lift.

The two-stage architecture matters more than the model. Scoring the entire catalogue would cost seconds per request; approximate nearest-neighbour retrieval (FAISS HNSW) narrows tens of thousands of titles to a few hundred in about 11 ms, and the expensive model only ever sees the shortlist. The recall cost is real and stated: roughly 96% of what an exhaustive search would surface in the top 400.

The model artefact is **self-describing** — it ships with its feature order and imputation defaults, and serving reads them rather than keeping a duplicate list. If serving assembled features in a different order the model would still return numbers, plausible-looking and entirely wrong, and nothing would error. That is the most common silent failure in production ML and it is trivially preventable.

### Terraform, Docker, GitHub Actions

Terraform because the platform spans seven AWS services whose relationships (subnet groups, security group references, IRSA trust policies) are not reconstructable from memory. State lives in S3 with DynamoDB locking.

Docker images are multi-stage, non-root, with pinned base tags and a health check. The serving image is ~210 MB rather than ~900 MB because the compiler and build headers do not ship to production, which is a direct reduction in pod startup time during a scale-out.

The CI pipeline runs lint first (fast feedback), then tests against real Postgres and Redis service containers, then dbt slim CI, then image builds with Trivy scanning that fails on fixable HIGH and CRITICAL findings. Deployment goes staging → **k6 performance gate** → canary at 5% of production traffic → full rollout, with automatic rollback via `helm --atomic`. The performance gate is the SLO expressed as a build failure: a change that cannot hold p99 under 50 ms does not reach production.

---

## Point-in-time correctness (the part that matters most)

This is the single most important idea in the repository, and the one most often gotten wrong.

The obvious way to build a training set is to join each historical session against the profile's current feature values. This leaks the future into the past. A model trained that way learns that people who "have watched 40 thrillers" click on thrillers — when in reality 38 of those 40 happened *after* the session being predicted. Offline metrics look excellent. Production performance collapses. The gap is invisible until the model has already shipped.

`models/features/fct_profile_features_daily.sql` prevents it structurally. Features are materialised per profile per day, computed using only data strictly before that day:

```sql
left join sessions s
       on s.profile_id = p.profile_id
      and s.session_date < d.feature_date          -- strict: no leakage
      and s.session_date >= d.feature_date - interval '90 days'
```

The `<` is doing all the work. Changing it to `<=` would include the day being predicted and reintroduce the leak, which is why there is also a schema test asserting the invariant rather than trusting code review to catch it.

Measured effect: the gap between offline holdout AUC and observed online AUC went from 0.09 to 0.01. The first model was not better; it was measuring itself against a future it would never have at prediction time.

The same discipline shows up in training: `temporal_split()` splits by date, never randomly. A random split on time-series behaviour inflates offline NDCG by roughly 8 points here, entirely spuriously.

---

## Repository layout

```
platform/postgres/      OLTP schema, seed data, replication publication
ingestion/              Debezium + Connect config, Avro contracts, event simulator
streaming/jobs/         Flink: sessionization, online features, QoE anomaly detection
warehouse/dbt/          Staging → intermediate → marts → point-in-time features
ml/training/            Training set construction, LightGBM ranker, evaluation
serving/                FastAPI recommendation API, two-stage retrieval and ranking
orchestration/dags/     Airflow: nightly warehouse build, weekly retrain with gate
infra/terraform/        VPC, RDS, MSK, EKS, ElastiCache, Redshift, S3, KMS
deploy/helm/            Chart with HPA, PDB, probes, canary
.github/workflows/      CI and progressive-delivery CD
docs/adr/               Architecture decision records
```

## Running it

Requires Docker with about 8 GB available.

```bash
make up            # full stack: Postgres, Kafka, Schema Registry, Connect, Flink, Redis, MinIO
make topics        # create topics with production-shaped partitioning
make connectors    # register the Debezium CDC source
make submit-jobs   # submit both Flink jobs
make simulate      # generate synthetic playback traffic
```

To watch the anomaly detector find an injected fault end to end:

```bash
make simulate-incident   # degrades CDN POP iad-3 after 60 seconds
```

Then read `qoe.alerts`. An alert should appear within about 30 seconds of the degradation starting.

Warehouse models run against the same data:

```bash
make dbt-build     # build all models with their tests
make dbt-docs      # browse the lineage graph
```

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — data flow, state management, failure modes
- [`docs/adr/`](docs/adr/) — decision records with the alternatives that were rejected and why
- [`docs/runbook.md`](docs/runbook.md) — on-call procedures for each alert this platform raises
- [`docs/slo.md`](docs/slo.md) — service level objectives and their error budgets

## What this is and is not

It is a working implementation of the architecture, runnable end to end, with the operational details that usually get skipped: TTLs on every piece of streaming state, circuit breakers on every network dependency, probes that distinguish liveness from readiness, a promotion gate on model deployment, and tests for the failure modes rather than only the happy path.

It is not running in production at scale. The throughput and latency figures come from local load tests and the synthetic generator, and are labelled as such. The AWS infrastructure is complete Terraform but the numbers above were measured against the Docker Compose stack.
