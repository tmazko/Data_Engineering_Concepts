select
    order_id,
    order_date,
    updated_at
from {{ ref('stg_orders') }}
where updated_at < order_date