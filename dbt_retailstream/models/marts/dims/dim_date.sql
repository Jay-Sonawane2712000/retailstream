with distinct_dates as (
    select distinct
        cast(order_purchase_timestamp as date) as full_date
    from {{ ref('stg_orders') }}
    where order_purchase_timestamp is not null
)

select
    cast(strftime(full_date, '%Y%m%d') as integer) as date_id,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    strftime(full_date, '%B') as month_name,
    weekofyear(full_date) as week,
    dayname(full_date) as day_of_week,
    case
        when dayofweek(full_date) in (0, 6) then true
        else false
    end as is_weekend
from distinct_dates
