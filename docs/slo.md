# Service level objectives

Each objective states what is promised, how it is measured, what the error
budget is, and what happens when the budget is spent. An SLO without a
consequence is a wish.

Windows are rolling 30 days unless stated otherwise.

---

## Recommendation API availability

**Objective:** 99.9% of requests return a non-5xx response.

**Measured** at the load balancer, not in the application. An application-side
metric cannot observe the requests that never reached a pod, which is precisely
the failure that matters most.

**Error budget:** 43 minutes per 30 days.

**Consequence when exhausted:** feature work on the serving path stops until
reliability work has restored the budget. Deployments continue — freezing
deploys after an incident usually delays the fix.

**Note on what counts.** A response served from the popularity fallback is a
success for this objective. It is degraded relevance, not an outage, and it is
tracked separately below.

---

## Recommendation latency

**Objective:** p99 under 50 ms, p50 under 15 ms.

**Measured** with the `recommendation_latency_seconds` histogram, using buckets
chosen around the objective rather than the client library defaults. Default
buckets put nearly every request into one bin at this latency and make the p99
unreadable.

**Error budget:** 1% of requests may exceed 50 ms.

**Why 50 ms.** This call sits on the critical path of the home screen render.
Above roughly 100 ms the delay becomes perceptible in the shelf-loading
animation; 50 ms leaves room for the rest of the page to assemble within
budget.

**Consequence:** the k6 gate in CI fails the deploy before it reaches
production, so a regression is caught at review time rather than by this SLO.
The SLO exists to catch the cases CI cannot reproduce — traffic mix shifts,
noisy neighbours, cache-cold pods after a scale-out.

---

## Feature freshness

**Objective:** 99% of playback events are reflected in the online feature store
within 5 seconds of emission.

**Measured** as the delta between `event_ts` on the source event and the Redis
write timestamp, sampled continuously by the Flink job's own metrics.

**Error budget:** 1% of events.

**Why 5 seconds.** Below about 10 seconds the personalisation is
indistinguishable from instant to a viewer browsing a home screen. Below 2
seconds is achievable only by shrinking the watermark bound, which costs
correctness on late mobile events — a bad trade for an improvement nobody can
perceive.

**Consequence:** the API's `served_from` label shows fallback usage rising.
Sustained breach means the streaming layer is under-provisioned, and the fix is
parallelism, not tuning.

---

## Warehouse freshness

**Objective:** marts reflect data up to the previous midnight by 06:00 UTC,
99% of days.

**Measured** by `dbt source freshness` plus the completion timestamp of the
`warehouse_elt` DAG.

**Error budget:** roughly one missed morning per quarter.

**Why 06:00.** European business hours start at 07:00 UTC and the first
dashboard loads land shortly after. A build finishing at 06:00 leaves an hour
to notice and rerun before anyone is looking at stale numbers.

---

## QoE detection latency

**Objective:** 95% of injected quality regressions are detected within 90
seconds of onset.

**Measured** by a synthetic fault injected weekly into a canary POP, timed from
injection to alert.

**Current performance:** 34 seconds median against injected faults.

**Why this is verified with synthetic faults.** Real incidents are too rare to
measure a detection rate against, and waiting for one to find out the detector
has silently broken is not a strategy. A weekly injection is the only way to
know the alerting path still works end to end.

---

## Model quality

**Objective:** NDCG@10 on the temporal holdout does not regress more than
0.005 between consecutive promoted models.

**Measured** at training time, gated in `model_retraining`.

**Consequence:** promotion is blocked automatically. This is not an alert; it
is a control.

**The failure this catches** is gradual drift rather than a single bad model.
Any individual retrain that is 0.004 worse looks like noise. Twelve of them in
a row is a materially worse recommender that nobody ever decided to ship.

---

## Online/offline feature parity

**Objective:** fewer than 1% of sampled sessions show more than 2% drift
between the Flink-computed and dbt-computed feature values.

**Measured** daily by `assert_online_offline_feature_parity`.

**Consequence:** a warning, deliberately not a build failure. Parity drift
should open an investigation; blocking the nightly build over a metric
definition disagreement would trade a subtle problem for an obvious outage.

**Why this has an SLO at all.** It is the failure mode with the longest time to
detection in the whole platform. Nothing breaks, no alert fires, and model
quality decays for weeks. Giving it an explicit objective is what converts it
from an invisible risk into something with an owner.
