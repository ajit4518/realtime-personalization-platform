{{ config(materialized='view') }}

/*
    Title-to-genre bridge.

    A title has several genres, and genre affinity is one of the stronger
    ranking features, so this is a real many-to-many rather than a single
    "primary genre" column. The first genre by id is treated as primary
    downstream purely for display and diversity capping.
*/

with ranked as (

    select
        *,
        row_number() over (
            partition by title_id, genre_id
            order by __lsn desc
        ) as _recency
    from {{ source('cdc', 'title_genres') }}

),

current_state as (

    select
        cast(title_id as bigint)   as title_id,
        cast(genre_id as smallint) as genre_id
    from ranked
    where _recency = 1
      and coalesce(__deleted, 'false') = 'false'

)

select
    title_id,
    genre_id,
    row_number() over (partition by title_id order by genre_id) = 1 as is_primary_genre
from current_state
