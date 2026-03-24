select
    promo_id,promo_name,
    cast(discount_pct as decimal(10,2)) as discount_pct
from {{ ref("raw_promotions") }}