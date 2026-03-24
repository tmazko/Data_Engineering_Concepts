select
    product_id, product_name, category, price, product_cost
from {{ ref('stg_products') }}