{{
    config(
        materialized='incremental',
        unique_key=['metric_date', 'cdn_pop'],
        incremental_strategy='merge',
        partition_by={'field': 'metric_date', 'data_type': 'date'}
    )
}}

/*
    Daily playback quality per CDN edge location.

    Two consumers with different needs:

    * On-call reads it during an incident to answer "is this one POP or the
      whole provider", which is the first question every time and the one the
      streaming alert cannot answer because it only sees one POP at a time.

    * Analytics reads it to correlate quality against retention, which is the
      argument that justifies CDN spend.

    Note this is the batch complement to the Flink detector, not a replacement.
    The detector finds regressions in seconds; this explains them afterwards
    with context the streaming path does not have, such as the provider a POP
    belongs to and how the same POP behaved on the same weekday historically.
*/

with sessions as (

    select * from {{ ref('fct_watch_sessions') }}
    where is_valid_session
      and cdn_pop is not null

    {% if is_incremental() %}
      and session_date >= (
          select coalesce(max(metric_date), '1900-01-01') - interval '{{ var("lookback_days") }} days'
          from {{ this }}
      )
    {% endif %}

),

daily as (

    select
        session_date                                        as metric_date,
        cdn_pop,
        region_code,

        count(*)                                            as sessions,
        count(distinct profile_id)                          as unique_viewers,
        sum(seconds_watched) / 3600.0                       as playback_hours,

        -- The headline quality metric, and the one the SLO is written against.
        sum(rebuffer_seconds) / nullif(sum(seconds_watched) / 3600.0, 0)
                                                            as rebuffer_seconds_per_hour,
        avg(case when rebuffer_count > 0 then 1.0 else 0.0 end)
                                                            as sessions_with_rebuffer_pct,

        approx_percentile(startup_ms, 0.50)                 as p50_startup_ms,
        approx_percentile(startup_ms, 0.95)                 as p95_startup_ms,
        approx_percentile(startup_ms, 0.99)                 as p99_startup_ms,

        avg(avg_bitrate_kbps)                               as avg_bitrate_kbps,
        avg(case when had_error then 1.0 else 0.0 end)      as error_rate,
        avg(case when explicitly_abandoned then 1.0 else 0.0 end)
                                                            as abandon_rate,
        avg(cdn_pop_switches)                               as avg_pop_switches

    from sessions
    group by session_date, cdn_pop, region_code

)

select
    d.*,
    r.cdn_provider,

    -- Same weekday last week. Comparing Saturday to Friday produces a
    -- difference that is real but entirely uninteresting.
    lag(d.rebuffer_seconds_per_hour, 7) over (
        partition by d.cdn_pop order by d.metric_date
    )                                                       as rebuffer_per_hour_same_day_last_week,

    avg(d.rebuffer_seconds_per_hour) over (
        partition by d.cdn_pop
        order by d.metric_date
        rows between 27 preceding and 1 preceding
    )                                                       as rebuffer_per_hour_28d_baseline,

    -- Deliberately coarser than the streaming detector's threshold. This
    -- classifies a day for reporting; the detector pages on a minute.
    case
        when d.rebuffer_seconds_per_hour > 60 then 'poor'
        when d.rebuffer_seconds_per_hour > 20 then 'degraded'
        when d.rebuffer_seconds_per_hour > 5  then 'acceptable'
        else 'good'
    end                                                     as quality_band,

    current_timestamp                                       as _built_at

from daily d
left join {{ ref('stg_regions') }} r on r.region_code = d.region_code
