with source_orders as (
    select
        order_id,
        customer_id,
        lower(trim(order_status)) as order_status,
        try_cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
        try_cast(order_approved_at as timestamp) as order_approved_at,
        try_cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
        try_cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        try_cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date
    from {{ source('bronze', 'bronze_orders') }}
    where order_id is not null
),

ranked_orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        row_number() over (
            partition by order_id
            order by order_purchase_timestamp desc nulls last
        ) as row_num
    from source_orders
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
from ranked_orders
where row_num = 1
