{{ config(materialized='view') }}

/*
    Current subscription state, reconstructed from the CDC stream.

    The raw topic holds every change ever made, including deletes (which
    Debezium rewrites into rows flagged `__deleted`). Taking the row with the
    highest log sequence number per key gives the same answer as querying the
    OLTP table directly, without adding read load to the production database.

    Using __lsn rather than a timestamp is deliberate: two updates inside the
    same millisecond are common under load, and the LSN is the only strictly
    monotonic ordering Postgres gives us.
*/

with ranked as (

    select
        *,
        row_number() over (
            partition by subscription_id
            order by __lsn desc
        ) as _recency
    from {{ source('cdc', 'subscriptions') }}

),

current_state as (

    select
        cast(subscription_id as bigint)     as subscription_id,
        cast(subscriber_id   as bigint)     as subscriber_id,
        tier,
        status,
        cast(monthly_price as decimal(8,2)) as monthly_price,
        cast(started_at as timestamp)       as started_at,
        cast(ended_at   as timestamp)       as ended_at,
        cast(updated_at as timestamp)       as updated_at,

        status = 'active'                   as is_active,

        case tier
            when 'basic'    then 1
            when 'standard' then 2
            when 'premium'  then 3
        end                                 as tier_rank

    from ranked
    where _recency = 1
      -- Deleted subscriptions are dropped here rather than upstream so the
      -- raw layer stays a faithful log of what actually happened.
      and coalesce(__deleted, 'false') = 'false'

)

select * from current_state
