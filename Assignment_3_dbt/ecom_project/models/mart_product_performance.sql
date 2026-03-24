with products_agg as (select p.product_id, p.product_name,
                         sum(i.quantity)                as items_sold,
                         sum(i.quantity * i.unit_price) as total_revenue
                  from {{ ref('fct_order_items') }} i
                  join {{ ref('dim_products') }} p on i.product_id = p.product_id
                  group by p.product_id, p.product_name)
select product_id, product_name, items_sold, total_revenue,
       avg(total_revenue) over(partition by product_id )
from products_agg