{{ config(materialized='view') }}

with ranked as (

    select *, row_number() over (partition by profile_id order by __lsn desc) as _recency
    from {{ source('cdc', 'profiles') }}

)

select
    cast(profile_id as bigint)      as profile_id,
    cast(subscriber_id as bigint)   as subscriber_id,
    display_name,
    cast(is_kids as boolean)        as is_kids,
    cast(created_at as timestamp)   as created_at,
    cast(updated_at as timestamp)   as updated_at
from ranked
where _recency = 1
  and coalesce(__deleted, 'false') = 'false'
