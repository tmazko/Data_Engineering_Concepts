select order_status, count(order_id) as count_status
from {{ ref('fct_orders') }}
group by order_status