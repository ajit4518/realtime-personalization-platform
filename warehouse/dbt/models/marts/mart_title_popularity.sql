{{
    config(
        materialized='incremental',
        unique_key=['title_id', 'metric_date'],
        incremental_strategy='merge',
        partition_by={'field': 'metric_date', 'data_type': 'date'}
    )
}}

/*
    Daily popularity score per title, used as a ranking feature and as the
    cold-start ordering when no profile embedding exists.

    Two properties this needs that a plain view count does not have:

    1. **Recency weighting.** Yesterday's viewing predicts tomorrow's far
       better than a view from two months ago. An unweighted total makes
       long-tail catalogue titles with years of accumulated views outrank a
       release that is currently the most watched thing on the service.

    2. **Point-in-time safety.** The score for a given date uses only data
       strictly before it, so the training join in `train_ranker.py` (which
       reads this at `session_date - 1 day`) cannot leak the outcome it is
       trying to predict.
*/

with daily_activity as (

    select
        title_id,
        session_date,
        count(*)                                            as sessions,
        count(distinct profile_id)                          as unique_viewers,
        sum(seconds_watched)                                as seconds_watched,
        avg(case when is_completed then 1.0 else 0.0 end)   as completion_rate
    from {{ ref('fct_watch_sessions') }}
    where is_valid_session
    group by title_id, session_date

),

spine as (

    select date_day as metric_date
    from {{ ref('dim_date') }}
    where date_day >= '{{ var("start_date") }}'
      and date_day < current_date

    {% if is_incremental() %}
      and date_day > (select coalesce(max(metric_date), '1900-01-01') from {{ this }})
    {% endif %}

),

windowed as (

    select
        s.metric_date,
        a.title_id,

        -- Exponential recency weighting with a 14-day half-life. Yesterday
        -- counts fully, a view a fortnight ago counts half, one from two
        -- months ago barely registers.
        sum(
            a.sessions * power(0.5, date_diff('day', a.session_date, s.metric_date) / 14.0)
        )                                                   as weighted_sessions,

        sum(a.sessions)     filter (where a.session_date >= s.metric_date - interval '7 days')  as sessions_7d,
        sum(a.unique_viewers) filter (where a.session_date >= s.metric_date - interval '7 days') as viewers_7d,
        sum(a.seconds_watched) filter (where a.session_date >= s.metric_date - interval '7 days') as seconds_watched_7d,
        avg(a.completion_rate) filter (where a.session_date >= s.metric_date - interval '30 days') as completion_rate_30d

    from spine s
    join daily_activity a
      on a.session_date < s.metric_date            -- strict: no leakage
     and a.session_date >= s.metric_date - interval '60 days'
    group by s.metric_date, a.title_id

)

select
    metric_date,
    title_id,

    coalesce(sessions_7d, 0)            as sessions_7d,
    coalesce(viewers_7d, 0)             as viewers_7d,
    coalesce(seconds_watched_7d, 0)     as seconds_watched_7d,
    coalesce(completion_rate_30d, 0)    as completion_rate_30d,
    round(weighted_sessions, 4)         as weighted_sessions,

    -- Normalised to 0..1 within each day so the score is comparable across
    -- dates even as total platform traffic grows.
    round(
        weighted_sessions
        / nullif(max(weighted_sessions) over (partition by metric_date), 0),
        6
    )                                   as popularity_score,

    row_number() over (
        partition by metric_date order by weighted_sessions desc
    )                                   as popularity_rank,

    current_timestamp                   as _built_at

from windowed
