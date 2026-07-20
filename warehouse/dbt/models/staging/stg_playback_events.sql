{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        cluster_by=['session_id']
    )
}}

/*
    Deduplicated, typed player telemetry.

    Two things happen here and nothing else:
      1. Client retries are collapsed. The same event_id genuinely arrives more
         than once because mobile SDKs retry on network failure; counting a
         retried HEARTBEAT twice inflates watch time by a few percent, which is
         enough to move a ranking model.
      2. Device clock skew is measured but not corrected. Some clients report
         event_ts minutes off from reality. We keep the raw value (the Flink
         watermarks depend on it) and expose the skew so downstream models can
         filter the pathological cases.
*/

with source as (

    select * from {{ source('events', 'playback_events') }}

    {% if is_incremental() %}
        -- Reprocess a few days on every run rather than only new partitions.
        -- Late-arriving mobile events routinely land a day after the fact, and
        -- a strict watermark here would silently drop them.
        where event_ts >= (
            select coalesce(max(event_ts), '1970-01-01'::timestamp)
                   - interval '{{ var("lookback_days") }} days'
            from {{ this }}
        )
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            -- Keep the earliest gateway receipt: it is the one whose ordering
            -- the streaming layer actually acted on.
            order by ingest_ts asc
        ) as _dedup_rank
    from source

),

typed as (

    select
        event_id,
        session_id,
        cast(profile_id as bigint)                  as profile_id,
        cast(title_id   as bigint)                  as title_id,
        region_code,
        event_type,

        cast(event_ts  as timestamp)                as event_ts,
        cast(ingest_ts as timestamp)                as ingest_ts,
        cast(event_ts as date)                      as event_date,

        -- Positive skew means the device clock runs ahead of the gateway.
        date_diff('second', cast(ingest_ts as timestamp), cast(event_ts as timestamp))
                                                    as clock_skew_seconds,

        cast(position_seconds as integer)           as position_seconds,
        device_type,
        app_version,
        cdn_pop,
        cast(bitrate_kbps as integer)               as bitrate_kbps,

        -- Stalls longer than a minute are not rebuffering, they are a client
        -- that went to sleep mid-report. Treating them as quality events makes
        -- the rebuffer ratio meaningless.
        case
            when rebuffer_ms between 0 and 60000 then cast(rebuffer_ms as integer)
            else null
        end                                         as rebuffer_ms,

        cast(startup_ms as integer)                 as startup_ms,
        error_code,
        coalesce(ab_bucket, 'control')              as ab_bucket

    from deduplicated
    where _dedup_rank = 1
      and event_ts >= '{{ var("start_date") }}'
      -- Guard against clients reporting from the future, which breaks
      -- incremental high-water marks permanently once it lands.
      and event_ts <= current_timestamp + interval '1 hour'

)

select * from typed
