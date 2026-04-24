select
    d.date_id,
    d.full_date,
    count(distinct f.order_id) as total_orders,
    sum(f.total_revenue) as total_revenue,
    avg(f.payment_value) as avg_order_value
from {{ ref('fact_orders') }} as f
inner join {{ ref('dim_date') }} as d
    on f.date_id = d.date_id
group by
    d.date_id,
    d.full_date
