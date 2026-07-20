{{ config(materialized='view') }}

-- Current catalogue state from the CDC stream. Same LSN-ordering pattern as
-- stg_subscriptions: highest log sequence number per key wins.

with ranked as (

    select *, row_number() over (partition by title_id order by __lsn desc) as _recency
    from {{ source('cdc', 'titles') }}

)

select
    cast(title_id as bigint)            as title_id,
    external_ref,
    title_name,
    content_type,
    cast(runtime_seconds as integer)    as runtime_seconds,
    cast(release_date as date)          as release_date,
    maturity_rating,
    cast(is_original as boolean)        as is_original,
    cast(licence_expires as date)       as licence_expires,
    cast(updated_at as timestamp)       as updated_at,

    -- Bucketing used by the ranker as a categorical feature; raw runtime is too
    -- high-cardinality to split on usefully.
    case
        when runtime_seconds < 900  then 'short'
        when runtime_seconds < 2700 then 'episode'
        when runtime_seconds < 5400 then 'feature'
        else 'long_feature'
    end                                 as runtime_bucket,

    date_diff('day', cast(release_date as date), current_date) as days_since_release

from ranked
where _recency = 1
  and coalesce(__deleted, 'false') = 'false'
