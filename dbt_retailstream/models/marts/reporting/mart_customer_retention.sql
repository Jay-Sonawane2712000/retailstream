select
    customer_id,
    count(distinct order_id) as total_orders,
    min(date_id) as first_order_date,
    max(date_id) as last_order_date,
    case
        when count(distinct order_id) > 1 then 1
        else 0
    end as repeat_customer_flag
from {{ ref('fact_orders') }}
group by customer_id
