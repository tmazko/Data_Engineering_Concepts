select u.city,
       count(o.order_id) as total_orders,
       sum(oi.quantity * oi.unit_price) as total_revenue
from {{ ref('fct_orders') }} o
join {{ ref('dim_users') }} u on o.user_id = u.user_id
join {{ ref('fct_order_items') }} oi on o.order_id=oi.order_id
group by u.city