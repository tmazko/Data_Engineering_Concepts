{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        incremental_predicates=["DBT_INTERNAL_DEST.order_date >= current_date - interval 30 day"]
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    user_id,
    order_status,
    promo_id,
    order_date,
    updated_at
from orders

{% if is_incremental() %}
    where order_date >= (select max(order_date) from {{ this }})
{% endif %}