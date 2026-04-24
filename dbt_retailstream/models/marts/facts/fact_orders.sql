with payments_by_order as (
    select
        order_id,
        sum(payment_value) as payment_value,
        min(payment_type) as payment_type
    from {{ ref('stg_payments') }}
    group by order_id
),

reviews_by_order as (
    select
        order_id,
        max(review_score) as review_score
    from {{ ref('stg_reviews') }}
    group by order_id
)

select
    o.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    cast(strftime(cast(o.order_purchase_timestamp as date), '%Y%m%d') as integer) as date_id,
    o.order_status,
    i.price as unit_price,
    i.freight_value,
    i.price + i.freight_value as total_revenue,
    p.payment_value,
    p.payment_type,
    datediff('day', o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_days,
    r.review_score
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_order_items') }} as i
    on o.order_id = i.order_id
left join {{ ref('stg_customers') }} as c
    on o.customer_id = c.customer_id
left join {{ ref('stg_products') }} as pr
    on i.product_id = pr.product_id
left join {{ ref('stg_sellers') }} as s
    on i.seller_id = s.seller_id
left join payments_by_order as p
    on o.order_id = p.order_id
left join reviews_by_order as r
    on o.order_id = r.order_id
where o.order_id is not null
  and i.order_item_id is not null
  and o.customer_id is not null
  and i.product_id is not null
  and i.seller_id is not null
