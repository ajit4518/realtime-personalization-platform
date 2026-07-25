{{
    config(
        materialized='incremental',
        unique_key=['title_id', 'metric_date'],
        incremental_strategy='merge',
        partition_by={'field': 'metric_date', 'data_type': 'date'}
    )
}}

/*
    Per-title, per-day performance. The table content strategy and finance both
    read, so the definitions here are the ones that end up in board decks and
    they need to survive scrutiny.

    Two decisions worth stating:

    1. `completion_rate` counts only valid sessions (30s+). Including accidental
       clicks understates completion for short-form content by roughly 12% and
       makes the whole catalogue look worse than it is.

    2. `efficiency_index` divides engagement by licence cost, which is how a
       renewal conversation actually gets framed. A title with modest viewership
       and a small licence fee can outperform a blockbuster, and a plain
       viewership ranking hides that entirely.
*/

with sessions as (

    select * from {{ ref('int_watch_sessions_enriched') }}
    where is_valid_session

    {% if is_incremental() %}
      and session_date >= (
          select coalesce(max(metric_date), '1900-01-01') - interval '{{ var("lookback_days") }} days'
          from {{ this }}
      )
    {% endif %}

),

daily as (

    select
        title_id,
        session_date                                        as metric_date,

        count(*)                                            as sessions,
        count(distinct profile_id)                          as unique_viewers,
        sum(seconds_watched)                                as total_seconds_watched,
        sum(seconds_watched) / 3600.0                       as total_hours_watched,

        avg(completion_ratio)                               as avg_completion_ratio,
        avg(case when is_completed then 1.0 else 0.0 end)   as completion_rate,
        approx_percentile(completion_ratio, 0.5)            as median_completion_ratio,

        -- Abandonment inside the first two minutes. The clearest signal that
        -- the artwork or synopsis is overselling the title.
        avg(case when seconds_watched < 120 then 1.0 else 0.0 end) as early_abandon_rate,

        avg(rebuffer_seconds_per_hour)                      as avg_rebuffer_per_hour,
        avg(case when had_error then 1.0 else 0.0 end)      as error_rate,
        approx_percentile(startup_ms, 0.95)                 as p95_startup_ms

    from sessions
    group by title_id, session_date

),

with_dimensions as (

    select
        d.*,
        t.title_name,
        t.content_type,
        t.is_original,
        t.runtime_seconds,
        t.release_date,
        t.licence_expires,
        date_diff('day', t.release_date, d.metric_date)     as days_since_release,

        c.licence_cost_usd,

        -- Repeat viewership: sessions beyond the first per viewer. High values
        -- on a series is healthy, on a film it usually means people are
        -- restarting because something failed.
        cast(d.sessions as double) / nullif(d.unique_viewers, 0) as sessions_per_viewer

    from daily d
    join {{ ref('stg_titles') }} t using (title_id)
    left join {{ ref('stg_title_costs') }} c using (title_id)

)

select
    *,

    case
        when licence_cost_usd > 0
        then round(total_hours_watched / (licence_cost_usd / 1000.0), 2)
    end                                                     as efficiency_index,

    -- Renewal urgency, surfaced here so the content team does not have to
    -- reconstruct it in a spreadsheet every quarter.
    case
        when licence_expires is null then 'owned'
        when licence_expires <= current_date + interval '90 days'
             and total_hours_watched > 0 then 'renew_review'
        when licence_expires <= current_date + interval '90 days' then 'let_lapse'
        else 'active'
    end                                                     as licence_status,

    current_timestamp                                       as _built_at

from with_dimensions
