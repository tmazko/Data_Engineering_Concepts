{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
 }}

select
    o.order_id, o.user_id, o.updated_at,
    sum(oi.quantity * oi.unit_price) as returned_amount
from {{ ref('fct_orders') }} o
join {{ ref('fct_order_items') }} oi on o.order_id=oi.order_id
where order_status = 'returned'
{% if is_incremental() %}
 and o.updated_at >= (select max(updated_at) from {{ this }})
{% endif %}
group by o.order_id, o.user_id, o.updated_at

