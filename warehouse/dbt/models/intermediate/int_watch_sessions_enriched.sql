{{ config(materialized='ephemeral') }}

/*
    Collapses raw player events into one row per viewing session.

    This is the offline mirror of what `streaming/jobs/session_features.py`
    computes online. The two implementations are independent on purpose (one is
    Flink state, one is SQL) but they must agree, so the arithmetic below is
    kept deliberately identical: heartbeats are worth 10 seconds, rebuffer
    seconds come from the same clamped column, completion uses the same
    threshold variable.

    `tests/assert_online_offline_feature_parity.sql` fails the build if the two
    diverge by more than tolerance on a sampled day.
*/

with events as (

    select * from {{ ref('stg_playback_events') }}

),

session_grain as (

    select
        session_id,
        min(profile_id)                                       as profile_id,
        min(title_id)                                         as title_id,
        min(region_code)                                      as region_code,
        min(device_type)                                      as device_type,
        min(app_version)                                      as app_version,
        min(ab_bucket)                                        as ab_bucket,

        min(event_ts)                                         as session_start_ts,
        max(event_ts)                                         as session_end_ts,
        cast(min(event_ts) as date)                           as session_date,

        -- Wall-clock span, which includes time spent paused.
        date_diff('second', min(event_ts), max(event_ts))     as session_span_seconds,

        -- Actual playback: each heartbeat represents 10 seconds of streaming.
        -- Deriving watch time from heartbeats rather than from the difference
        -- between first and last event is what makes paused sessions correct.
        count(*) filter (where event_type = 'HEARTBEAT') * 10 as seconds_watched,

        max(position_seconds)                                 as max_position_seconds,

        count(*) filter (where event_type = 'SEEK')           as seek_count,
        count(*) filter (where event_type = 'PAUSE')          as pause_count,
        count(*) filter (where event_type = 'REBUFFER')       as rebuffer_count,
        coalesce(sum(rebuffer_ms), 0) / 1000.0                as rebuffer_seconds,

        max(startup_ms) filter (where event_type = 'START')   as startup_ms,
        avg(bitrate_kbps)                                     as avg_bitrate_kbps,
        min(bitrate_kbps)                                     as min_bitrate_kbps,

        count(*) filter (where event_type = 'ERROR') > 0      as had_error,
        max(error_code)                                       as error_code,
        count(*) filter (where event_type = 'COMPLETE') > 0   as reached_complete,
        count(*) filter (where event_type = 'ABANDON') > 0    as explicitly_abandoned,

        -- The dimension that matters most during a quality incident.
        min(cdn_pop)                                          as cdn_pop,
        count(distinct cdn_pop)                               as cdn_pop_switches,

        count(*)                                              as event_count

    from events
    group by session_id

),

with_title_context as (

    select
        s.*,
        t.title_name,
        t.content_type,
        t.runtime_seconds,
        t.is_original,
        t.maturity_rating,

        -- Completion measured against the title's true runtime, capped because
        -- a viewer who rewatches a scene can exceed it.
        least(
            1.0,
            cast(s.seconds_watched as double) / nullif(t.runtime_seconds, 0)
        )                                                     as completion_ratio,

        -- Rebuffer seconds per hour of playback. The industry-comparable form
        -- of the quality metric, and the one the SLO is written against.
        case
            when s.seconds_watched > 0
            then s.rebuffer_seconds / (s.seconds_watched / 3600.0)
            else 0
        end                                                   as rebuffer_seconds_per_hour

    from session_grain s
    left join {{ ref('stg_titles') }} t using (title_id)

)

select
    *,
    completion_ratio >= {{ var('completion_threshold') }}     as is_completed,
    seconds_watched >= {{ var('min_valid_session_seconds') }} as is_valid_session,

    -- Label used by the ranker: a session counts as a positive signal when the
    -- viewer stayed materially engaged, not merely when they pressed play.
    case
        when completion_ratio >= {{ var('completion_threshold') }} then 1
        when completion_ratio >= 0.25 and not explicitly_abandoned then 1
        else 0
    end                                                       as engagement_label

from with_title_context
