select pr.promo_name, count(o.order_id) as times_used
from {{ ref('fct_orders') }} o
join {{ ref('dim_promotions') }} pr on o.promo_id = pr.promo_id
group by pr.promo_name