select
    order_id,
    user_id,
    order_date,
    lag(order_date) over (partition by user_id order by order_date) as previous_order_date
from {{ ref('fct_orders') }}