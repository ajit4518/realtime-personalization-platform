{{ config(materialized='view') }}

/*
    Licence and production cost per title.

    Sourced from the finance system rather than the product database, which is
    why it is a seed rather than a CDC stream: the feed arrives as a monthly
    file and the volume is a few thousand rows.

    Costs are amortised across the licence window rather than charged to the
    month of acquisition. A title licensed for three years is not a cost the
    quarter it lands, and treating it that way makes every efficiency metric
    in `mart_content_performance` misleading.
*/

with source as (

    select * from {{ ref('title_costs_seed') }}

)

select
    cast(title_id as bigint)                        as title_id,
    cast(licence_cost_usd as decimal(14,2))         as licence_cost_usd,
    cast(production_cost_usd as decimal(14,2))      as production_cost_usd,
    cast(licence_start as date)                     as licence_start,
    cast(licence_end as date)                       as licence_end,
    cost_type,

    greatest(date_diff('day', cast(licence_start as date), cast(licence_end as date)), 1)
                                                    as licence_days,

    round(
        cast(licence_cost_usd as decimal(14,2))
        / greatest(date_diff('day', cast(licence_start as date), cast(licence_end as date)), 1),
        4
    )                                               as daily_amortised_cost_usd

from source
