{{ config(materialized='view') }}

-- Genre reference data. Small and slow-changing, so it is read straight from
-- the CDC stream with the same LSN-recency pattern as every other staging model.

with ranked as (

    select *, row_number() over (partition by genre_id order by __lsn desc) as _recency
    from {{ source('cdc', 'genres') }}

)

select
    cast(genre_id as smallint) as genre_id,
    genre_name
from ranked
where _recency = 1
  and coalesce(__deleted, 'false') = 'false'
