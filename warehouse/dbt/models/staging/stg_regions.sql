{{ config(materialized='view') }}

-- Region reference data. Small, slow-changing, and joined by almost every mart,
-- so it is materialised as a view over the latest CDC state rather than seeded.

with ranked as (

    select *, row_number() over (partition by region_id order by __lsn desc) as _recency
    from {{ source('cdc', 'regions') }}

)

select
    cast(region_id as smallint) as region_id,
    region_code,
    display_name,
    cdn_provider
from ranked
where _recency = 1
  and coalesce(__deleted, 'false') = 'false'
