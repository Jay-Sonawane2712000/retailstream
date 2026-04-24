select distinct
    seller_id,
    try_cast(seller_zip_code_prefix as integer) as seller_zip_code_prefix,
    upper(trim(seller_city)) as seller_city,
    upper(trim(seller_state)) as seller_state
from {{ source('bronze', 'bronze_sellers') }}
where seller_id is not null
