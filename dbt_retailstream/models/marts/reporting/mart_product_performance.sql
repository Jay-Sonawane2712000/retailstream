select
    product_id,
    count(distinct order_id) as total_orders,
    sum(total_revenue) as total_revenue,
    avg(unit_price) as avg_price
from {{ ref('fact_orders') }}
group by product_id
