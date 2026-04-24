select
    seller_id,
    count(distinct order_id) as total_orders,
    sum(total_revenue) as total_revenue,
    avg(delivery_days) as avg_delivery_days
from {{ ref('fact_orders') }}
group by seller_id
