select distinct
    customer_id,
    customer_unique_id,
    try_cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
    upper(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state
from {{ source('bronze', 'bronze_customers') }}
where customer_id is not null
  and customer_unique_id is not null
