select
    promo_id, promo_name, discount_pct
from {{ ref('stg_promotions') }}