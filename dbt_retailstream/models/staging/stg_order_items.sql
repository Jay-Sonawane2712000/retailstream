select distinct
    order_id,
    try_cast(order_item_id as integer) as order_item_id,
    product_id,
    seller_id,
    try_cast(shipping_limit_date as timestamp) as shipping_limit_date,
    try_cast(price as double) as price,
    try_cast(freight_value as double) as freight_value
from {{ source('bronze', 'bronze_order_items') }}
where order_id is not null
  and product_id is not null
  and seller_id is not null
  and try_cast(price as double) > 0
