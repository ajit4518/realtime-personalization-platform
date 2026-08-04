# ADR 0002: Dual-path feature computation with automated parity testing

**Status:** Accepted
**Date:** 2026-07-21

## Context

Features are needed in two places with incompatible requirements.

**Serving** needs them in single-digit milliseconds, reflecting activity from seconds ago, for one profile at a time.

**Training** needs them over months of history, for millions of profiles, as of arbitrary past dates, with point-in-time correctness.

No single system does both well. A store fast enough for the first is not a system you scan ninety days of history from; a warehouse that answers the second takes seconds per query.

This is the well-known online/offline skew problem, and its failure mode is unusually nasty: when the two implementations disagree, **nothing breaks**. No job fails. No alert fires. The model simply trains on one distribution and serves against another, and ranking quality decays over weeks until someone notices a click-through slide and spends a fortnight hunting for a cause.

## Decision

Compute features twice — Flink for online, dbt for offline — and test that the two agree, daily and automatically.

The parity test (`warehouse/dbt/tests/assert_online_offline_feature_parity.sql`) samples the previous day, recomputes from the warehouse, and compares against what Flink actually wrote to Redis.

## Reasoning

**Why not compute once and share.** Two approaches exist and both were rejected.

Writing warehouse-computed features to Redis nightly means serving features up to 24 hours stale, which discards the freshness that justifies the streaming layer at all. This is kept as the *fallback* path, not the primary one.

Reading online features to build training data means training on whatever happened to be in Redis, which cannot be reconstructed for past dates and cannot be made point-in-time correct. It also makes the training set unreproducible, which makes debugging a model regression impossible.

**Why testing beats a shared implementation.** A managed feature store (Feast, Tecton) solves this by owning both paths from one definition, which is genuinely the better answer at sufficient scale. It was rejected here because it adds a substantial operational component to solve a problem that two implementations plus one test also solve, and because the transformations are not the same shape in both worlds anyway — Flink maintains incremental state, dbt does set-based aggregation over history.

**Tolerance is 2%, not exact equality.** The two paths legitimately disagree at the margin: Flink sees a stream truncated at the watermark, the warehouse sees late arrivals that landed hours later. A gap wider than 2% is a bug; a gap narrower is lateness.

**Severity is `warn`, not `error`.** A parity drift should open an investigation, not block the nightly build. Failing the DAG would mean every downstream mart goes stale because of a metric-definition disagreement, which trades a subtle problem for an obvious outage.

## Consequences

**Accepted costs.** Every feature is implemented twice, and both implementations must change together. This is real duplication and it will occasionally be gotten wrong — which is precisely what the test is for.

**Mitigations.** Both implementations carry comments pointing at each other. The arithmetic is kept deliberately identical (heartbeats are 10 seconds in both, the same clamped rebuffer column, the same completion threshold variable). Feature additions are expected to touch both files in one pull request, and CI does not enforce this — a reviewer does.

**Revisit if** the feature count grows past roughly 50, at which point the duplication cost likely exceeds the operational cost of a managed feature store.
