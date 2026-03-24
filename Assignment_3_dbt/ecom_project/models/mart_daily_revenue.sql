{{
    config(
        materialized='incremental',
        unique_key='report_date'
    )
 }}

select
    o.order_date as report_date,
    count(distinct o.order_id) as total_orders,
    sum(oi.quantity * oi.unit_price) as total_revenue
from {{ ref('fct_orders') }} o
join {{ ref('fct_order_items') }} oi on o.order_id=oi.order_id
where order_status = 'delivered'
{% if is_incremental() %}
    and o.order_date >= (select max(report_date) from {{ this }})
{% endif %}
group by o.order_date
