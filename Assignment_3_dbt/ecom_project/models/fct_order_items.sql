{{
    config(
        materialized='incremental',
        unique_key='order_item_id'
    )
 }}

select * from {{ ref("stg_order_items") }}

{% if is_incremental() %}
        where order_item_id > (select coalesce(max(order_item_id), 0) from {{ this }})
{% endif %}

