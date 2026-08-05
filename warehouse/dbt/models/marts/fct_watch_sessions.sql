{{
    config(
        materialized='incremental',
        unique_key='session_id',
        incremental_strategy='merge',
        partition_by={'field': 'session_date', 'data_type': 'date'},
        cluster_by=['profile_id']
    )
}}

/*
    Session-grain fact table. The persisted form of `int_watch_sessions_enriched`,
    which is ephemeral.

    Materialised because three separate consumers read it — the training set
    builder, the content mart, and ad-hoc analysis — and recomputing the
    sessionization three times per run costs more than storing it once.

    Clustered on profile_id because the dominant access pattern is "this
    profile's history", both for training joins and for debugging a specific
    viewer's recommendations.
*/

with enriched as (

    select * from {{ ref('int_watch_sessions_enriched') }}

    {% if is_incremental() %}
      where session_date >= (
          select coalesce(max(session_date), '1900-01-01') - interval '{{ var("lookback_days") }} days'
          from {{ this }}
      )
    {% endif %}

)

select
    session_id,
    profile_id,
    title_id,
    session_date,
    session_start_ts,
    session_end_ts,

    region_code,
    device_type,
    app_version,
    ab_bucket,
    cdn_pop,

    seconds_watched,
    session_span_seconds,
    completion_ratio,
    max_position_seconds,

    seek_count,
    pause_count,
    rebuffer_count,
    rebuffer_seconds,
    rebuffer_seconds_per_hour,
    startup_ms,
    avg_bitrate_kbps,
    min_bitrate_kbps,
    cdn_pop_switches,

    had_error,
    error_code,
    reached_complete,
    explicitly_abandoned,

    is_completed,
    is_valid_session,
    engagement_label,

    content_type,
    runtime_seconds,
    is_original,

    current_timestamp as _built_at

from enriched
