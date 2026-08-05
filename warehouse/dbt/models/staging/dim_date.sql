{{ config(materialized='table') }}

/*
    Date spine.

    Exists because the point-in-time feature model needs one row per day
    whether or not anything happened that day. Deriving the spine from observed
    activity would silently skip quiet days, and a gap in the spine becomes a
    gap in the training set that nobody notices until a model behaves oddly
    around a holiday.
*/

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('start_date') ~ "' as date)",
        end_date="dateadd(day, 1, current_date)"
    ) }}

)

select
    cast(date_day as date)                          as date_day,
    extract(year    from date_day)                  as year,
    extract(quarter from date_day)                  as quarter,
    extract(month   from date_day)                  as month,
    extract(day     from date_day)                  as day_of_month,
    extract(dow     from date_day)                  as day_of_week,
    to_char(date_day, 'Day')                        as day_name,

    -- Viewing behaviour differs sharply between weekdays and weekends, and
    -- models that ignore it misread every Saturday as an anomaly.
    extract(dow from date_day) in (0, 6)            as is_weekend,

    date_trunc('week',  date_day)::date             as week_start,
    date_trunc('month', date_day)::date             as month_start,

    date_day = current_date                         as is_today,
    date_day < current_date                         as is_complete_day

from spine
