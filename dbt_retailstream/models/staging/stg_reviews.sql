with source_reviews as (
    select
        review_id,
        order_id,
        try_cast(review_score as integer) as review_score,
        review_comment_title,
        review_comment_message,
        try_cast(review_creation_date as timestamp) as review_creation_date,
        try_cast(review_answer_timestamp as timestamp) as review_answer_timestamp
    from {{ source('bronze', 'bronze_reviews') }}
    where review_id is not null
      and order_id is not null
),

ranked_reviews as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        row_number() over (
            partition by review_id
            order by review_answer_timestamp desc nulls last
        ) as row_num
    from source_reviews
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
from ranked_reviews
where row_num = 1
