{{
    config(
        materialized='incremental',
        unique_key=['profile_id', 'feature_date'],
        incremental_strategy='merge',
        partition_by={'field': 'feature_date', 'data_type': 'date'}
    )
}}

/*
    Daily snapshot of every profile's features, as they were known at the END
    of that day and no later.

    This is the model that makes training honest.

    The failure it prevents: the obvious way to build a training set is to join
    each historical session against the profile's current feature values. That
    leaks the future into the past. A model trained that way learns that people
    who "have watched 40 thrillers" click on thrillers, when in reality 38 of
    those 40 happened *after* the session being predicted. Offline AUC looks
    excellent, production performance collapses, and the gap is invisible until
    the model ships.

    The fix is to materialise features per day using only data available up to
    that day, then join training labels on (profile_id, event_date) so every
    row sees the world as it actually looked at prediction time. Doing this
    moved our offline-to-online AUC gap from 0.09 to 0.01.

    Cost note: recomputing every profile for every day is quadratic. This model
    is incremental and only builds the days it is missing, so a normal run
    touches one partition.
*/

{% set feature_windows = [7, 30, 90] %}

with date_spine as (

    select date_day as feature_date
    from {{ ref('dim_date') }}
    where date_day >= '{{ var("start_date") }}'
      and date_day < current_date

    {% if is_incremental() %}
      and date_day > (select coalesce(max(feature_date), '1900-01-01') from {{ this }})
    {% endif %}

),

sessions as (

    select
        profile_id,
        session_date,
        title_id,
        content_type,
        seconds_watched,
        completion_ratio,
        is_completed,
        rebuffer_seconds_per_hour,
        device_type,
        engagement_label
    from {{ ref('int_watch_sessions_enriched') }}
    where is_valid_session

),

genre_sessions as (

    select
        s.profile_id,
        s.session_date,
        g.genre_name,
        s.seconds_watched
    from sessions s
    join {{ ref('stg_title_genres') }} tg using (title_id)
    join {{ ref('stg_genres') }} g using (genre_id)

),

/*
    The point-in-time join. Every aggregate below is bounded by
    `s.session_date < d.feature_date`, strictly less than, never <=. Using <=
    would include the day being predicted and reintroduce the leak this whole
    model exists to prevent.
*/
profile_windows as (

    select
        d.feature_date,
        p.profile_id,

        {% for window in feature_windows %}
        count(distinct s.session_date) filter (
            where s.session_date >= d.feature_date - interval '{{ window }} days'
        )                                                   as active_days_{{ window }}d,

        count(s.title_id) filter (
            where s.session_date >= d.feature_date - interval '{{ window }} days'
        )                                                   as sessions_{{ window }}d,

        count(distinct s.title_id) filter (
            where s.session_date >= d.feature_date - interval '{{ window }} days'
        )                                                   as distinct_titles_{{ window }}d,

        coalesce(sum(s.seconds_watched) filter (
            where s.session_date >= d.feature_date - interval '{{ window }} days'
        ), 0)                                               as seconds_watched_{{ window }}d,

        coalesce(avg(s.completion_ratio) filter (
            where s.session_date >= d.feature_date - interval '{{ window }} days'
        ), 0)                                               as avg_completion_{{ window }}d,

        coalesce(avg(case when s.is_completed then 1.0 else 0.0 end) filter (
            where s.session_date >= d.feature_date - interval '{{ window }} days'
        ), 0)                                               as completion_rate_{{ window }}d
        {{ "," if not loop.last }}
        {% endfor %},

        -- Quality experienced recently. A viewer who has been rebuffering is a
        -- churn risk regardless of what they watched, and the ranker should
        -- avoid pushing them toward high-bitrate content.
        coalesce(avg(s.rebuffer_seconds_per_hour) filter (
            where s.session_date >= d.feature_date - interval '7 days'
        ), 0)                                               as avg_rebuffer_per_hour_7d,

        -- Recency, the single strongest churn predictor in this dataset.
        date_diff('day', max(s.session_date), d.feature_date) as days_since_last_session,

        min(s.session_date)                                 as first_session_date

    from date_spine d
    cross join {{ ref('stg_profiles') }} p
    left join sessions s
           on s.profile_id = p.profile_id
          and s.session_date < d.feature_date          -- strict: no leakage
          and s.session_date >= d.feature_date - interval '90 days'
    group by d.feature_date, p.profile_id

),

genre_affinity as (

    select
        d.feature_date,
        gs.profile_id,
        -- Share of watch time per genre over the trailing 90 days, as a map the
        -- serving layer can read without a join.
        map_agg(
            gs.genre_name,
            round(
                cast(sum(gs.seconds_watched) as double)
                / nullif(sum(sum(gs.seconds_watched)) over (partition by d.feature_date, gs.profile_id), 0),
                4
            )
        )                                                   as genre_affinity
    from date_spine d
    join genre_sessions gs
      on gs.session_date < d.feature_date
     and gs.session_date >= d.feature_date - interval '90 days'
    group by d.feature_date, gs.profile_id

),

subscription_context as (

    select
        d.feature_date,
        sub.subscriber_id,
        sub.tier,
        sub.tier_rank,
        sub.is_active,
        date_diff('day', cast(sub.started_at as date), d.feature_date) as tenure_days
    from date_spine d
    -- The snapshot, not the current table: tier as of the feature date.
    join {{ ref('snap_subscriptions') }} sub
      on d.feature_date >= cast(sub.dbt_valid_from as date)
     and (sub.dbt_valid_to is null or d.feature_date < cast(sub.dbt_valid_to as date))

)

select
    pw.feature_date,
    pw.profile_id,
    p.subscriber_id,

    {% for window in feature_windows %}
    pw.active_days_{{ window }}d,
    pw.sessions_{{ window }}d,
    pw.distinct_titles_{{ window }}d,
    pw.seconds_watched_{{ window }}d,
    pw.avg_completion_{{ window }}d,
    pw.completion_rate_{{ window }}d,
    {% endfor %}

    pw.avg_rebuffer_per_hour_7d,
    coalesce(pw.days_since_last_session, 999)               as days_since_last_session,
    coalesce(date_diff('day', pw.first_session_date, pw.feature_date), 0) as profile_age_days,

    coalesce(ga.genre_affinity, map())                      as genre_affinity,

    sc.tier,
    sc.tier_rank,
    sc.is_active                                            as subscription_active,
    coalesce(sc.tenure_days, 0)                             as tenure_days,

    -- Derived signals the ranker consumes directly.
    case
        when pw.sessions_7d = 0 then 'dormant'
        when pw.active_days_7d >= 5 then 'daily'
        when pw.active_days_7d >= 2 then 'regular'
        else 'occasional'
    end                                                     as engagement_segment,

    current_timestamp                                       as _built_at

from profile_windows pw
join {{ ref('stg_profiles') }} p using (profile_id)
left join genre_affinity ga
       on ga.profile_id = pw.profile_id and ga.feature_date = pw.feature_date
left join subscription_context sc
       on sc.subscriber_id = p.subscriber_id and sc.feature_date = pw.feature_date
