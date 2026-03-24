select
    order_id,user_id,status as order_status,promo_id,order_date,updated_at
from {{ ref("raw_orders") }}