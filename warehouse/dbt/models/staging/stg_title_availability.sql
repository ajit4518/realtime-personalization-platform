{{ config(materialized='view') }}

/*
    Region- and time-scoped catalogue availability.

    The recommender reads this to avoid surfacing titles a viewer cannot play.
    Getting it wrong is a visible product bug: a row of artwork that returns
    "not available in your region" when tapped.
*/

with ranked as (

    select
        *,
        row_number() over (
            partition by title_id, region_id order by __lsn desc
        ) as _recency
    from {{ source('cdc', 'title_availability') }}

)

select
    cast(title_id  as bigint)        as title_id,
    cast(region_id as smallint)      as region_id,
    cast(available_from as timestamp) as available_from,
    cast(available_to   as timestamp) as available_to,

    -- Evaluated at query time rather than materialised, because a licence
    -- expiring overnight must take effect without waiting for a rebuild.
    current_timestamp >= cast(available_from as timestamp)
        and (available_to is null or current_timestamp < cast(available_to as timestamp))
                                     as is_available_now

from ranked
where _recency = 1
  and coalesce(__deleted, 'false') = 'false'
