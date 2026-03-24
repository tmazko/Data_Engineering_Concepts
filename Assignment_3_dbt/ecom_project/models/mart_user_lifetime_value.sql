select
    u.user_id, u.first_name, u.last_name, u.city,
    count(distinct o.order_id) as total_orders,
    min(o.order_date) as first_purchase_date,
    max(o.order_date) as last_purchase_date,
    sum(i.quantity * i.unit_price) as lifetime_revenue
from {{ ref('dim_users') }} u
left join {{ ref('fct_orders') }} o on u.user_id = o.user_id
left join {{ ref('fct_order_items') }} i on o.order_id = i.order_id
where o.order_status = 'delivered'
group by u.user_id, u.first_name, u.last_name, u.city