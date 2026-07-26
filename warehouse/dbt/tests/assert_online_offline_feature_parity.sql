{{ config(severity='warn', store_failures=true, tags=['parity', 'daily']) }}

/*
    Guards the single most dangerous failure mode in this platform:
    the Flink job and the dbt models silently computing different numbers for
    the same feature.

    When that happens, nothing breaks. No job fails, no alert fires. The model
    simply trains on one distribution and serves against another, and ranking
    quality decays over weeks until someone notices the click-through slide and
    spends a fortnight hunting for a cause.

    So we check it explicitly. The Flink job snapshots what it wrote to Redis
    into `raw_events.online_feature_audit`; this test recomputes the same
    sessions from the warehouse and compares.

    Tolerance is 2% rather than exact equality, because the two paths legitimately
    disagree at the margin: Flink sees an event stream truncated at the watermark,
    while the warehouse sees late arrivals that landed hours afterwards. A gap
    wider than that is a bug, not lateness.

    Severity is `warn` on purpose. A parity drift should open an investigation,
    not block the nightly warehouse build; failing the DAG here would mean every
    downstream mart goes stale because of a metric-definition disagreement.
*/

{% set tolerance = 0.02 %}
{% set min_sessions_to_judge = 500 %}

with online as (

    select
        session_id,
        profile_id,
        seconds_watched    as online_seconds_watched,
        rebuffer_seconds   as online_rebuffer_seconds,
        completion_ratio   as online_completion_ratio,
        snapshot_date
    from {{ source('events', 'online_feature_audit') }}
    -- Yesterday only: today's sessions are still open in Flink state and would
    -- fail the comparison for a reason that is not a defect.
    where snapshot_date = current_date - interval '1 day'

),

offline as (

    select
        session_id,
        profile_id,
        seconds_watched    as offline_seconds_watched,
        rebuffer_seconds   as offline_rebuffer_seconds,
        completion_ratio   as offline_completion_ratio
    from {{ ref('int_watch_sessions_enriched') }}
    where session_date = current_date - interval '1 day'
      and is_valid_session

),

compared as (

    select
        on_.session_id,
        on_.profile_id,

        on_.online_seconds_watched,
        off.offline_seconds_watched,
        abs(on_.online_seconds_watched - off.offline_seconds_watched)
            / nullif(off.offline_seconds_watched, 0)       as seconds_watched_drift,

        on_.online_rebuffer_seconds,
        off.offline_rebuffer_seconds,
        abs(on_.online_rebuffer_seconds - off.offline_rebuffer_seconds)
            / nullif(off.offline_rebuffer_seconds, 0)      as rebuffer_drift,

        on_.online_completion_ratio,
        off.offline_completion_ratio,
        abs(on_.online_completion_ratio - off.offline_completion_ratio) as completion_drift

    from online on_
    inner join offline off using (session_id)

),

-- Only judge when the sample is large enough to mean anything. On a quiet day
-- a handful of edge-case sessions would otherwise look like systemic drift.
sample_size as (
    select count(*) as n from compared
),

violations as (

    select
        c.session_id,
        c.profile_id,
        c.online_seconds_watched,
        c.offline_seconds_watched,
        c.seconds_watched_drift,
        c.rebuffer_drift,
        c.completion_drift,
        case
            when c.seconds_watched_drift > {{ tolerance }} then 'seconds_watched'
            when c.rebuffer_drift        > {{ tolerance }} then 'rebuffer_seconds'
            else 'completion_ratio'
        end as drifting_feature
    from compared c
    cross join sample_size s
    where s.n >= {{ min_sessions_to_judge }}
      and (
            c.seconds_watched_drift > {{ tolerance }}
         or c.rebuffer_drift        > {{ tolerance }}
         or c.completion_drift      > {{ tolerance }}
      )

)

-- Fail only if drift is widespread. One divergent session is a late event;
-- one percent of them is a definition that has come apart.
select * from violations
where (select count(*) from violations) > (select n * 0.01 from sample_size)
