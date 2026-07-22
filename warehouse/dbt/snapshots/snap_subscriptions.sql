{% snapshot snap_subscriptions %}

{{
    config(
        target_schema='snapshots',
        unique_key='subscription_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

/*
    Slowly changing dimension over subscription state.

    The OLTP table holds only the current row: when someone downgrades from
    premium to basic, the previous tier is overwritten and gone. That history is
    exactly what churn analysis needs, and what the point-in-time feature model
    reads to answer "which tier was this person on that day".

    Debezium does capture the before-image (REPLICA IDENTITY FULL is set on this
    table for that reason), but reconstructing validity ranges from a change log
    on every query is expensive and easy to get wrong. A snapshot materialises
    the ranges once.

    `invalidate_hard_deletes` matters here: an account deletion under GDPR
    removes the row entirely, and without this the snapshot would keep showing
    the subscription as indefinitely valid.
*/

select
    subscription_id,
    subscriber_id,
    tier,
    status,
    monthly_price,
    started_at,
    ended_at,
    updated_at
from {{ ref('stg_subscriptions') }}

{% endsnapshot %}
