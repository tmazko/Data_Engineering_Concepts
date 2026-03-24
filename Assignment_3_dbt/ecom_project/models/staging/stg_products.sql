select
    product_id, name as product_name, category,
    cast(price as decimal(10,2)) as price,
    cast(cost as decimal(10,2)) as product_cost
from {{ ref('raw_products') }}