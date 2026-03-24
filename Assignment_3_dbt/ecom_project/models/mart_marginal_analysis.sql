select
    i.order_id,
    sum({{ calculate_profit('i.unit_price', 'p.product_cost') }} * i.quantity) as profit
from {{ ref('fct_order_items') }} i
join {{ ref('dim_products') }} p on i.product_id = p.product_id
group by i.order_id