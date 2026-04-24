from pathlib import Path

import duckdb


DB_PATH = Path("data/retailstream.duckdb")


def create_silver_table(
    conn: duckdb.DuckDBPyConnection, table_name: str, select_sql: str
) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS {select_sql}")

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]
    print(f"{table_name} row count: {row_count}")


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))

    try:
        # Clean order-level data and keep one latest row per order_id.
        create_silver_table(
            conn,
            "silver_orders",
            """
            WITH ranked_orders AS (
                SELECT
                    order_id,
                    customer_id,
                    LOWER(TRIM(order_status)) AS order_status,
                    TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
                    TRY_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
                    TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
                    TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
                    TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY order_id
                        ORDER BY
                            TRY_CAST(order_purchase_timestamp AS TIMESTAMP) DESC NULLS LAST,
                            TRY_CAST(order_approved_at AS TIMESTAMP) DESC NULLS LAST,
                            TRY_CAST(order_delivered_customer_date AS TIMESTAMP) DESC NULLS LAST
                    ) AS row_num
                FROM bronze_orders
                WHERE order_id IS NOT NULL
                  AND customer_id IS NOT NULL
            )
            SELECT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date
            FROM ranked_orders
            WHERE row_num = 1
            """,
        )

        # Clean order item rows and remove exact duplicates.
        create_silver_table(
            conn,
            "silver_order_items",
            """
            SELECT DISTINCT
                order_id,
                TRY_CAST(order_item_id AS INTEGER) AS order_item_id,
                product_id,
                seller_id,
                TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
                TRY_CAST(price AS DOUBLE) AS price,
                TRY_CAST(freight_value AS DOUBLE) AS freight_value
            FROM bronze_order_items
            WHERE order_id IS NOT NULL
              AND order_item_id IS NOT NULL
              AND product_id IS NOT NULL
              AND seller_id IS NOT NULL
            """,
        )

        # Clean customer reference data and standardize city/state text.
        create_silver_table(
            conn,
            "silver_customers",
            """
            SELECT DISTINCT
                customer_id,
                customer_unique_id,
                TRY_CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
                LOWER(TRIM(customer_city)) AS customer_city,
                UPPER(TRIM(customer_state)) AS customer_state
            FROM bronze_customers
            WHERE customer_id IS NOT NULL
              AND customer_unique_id IS NOT NULL
            """,
        )

        # Clean product records and keep core product identifiers.
        create_silver_table(
            conn,
            "silver_products",
            """
            SELECT DISTINCT
                product_id,
                LOWER(TRIM(product_category_name)) AS product_category_name,
                TRY_CAST(product_name_lenght AS DOUBLE) AS product_name_lenght,
                TRY_CAST(product_description_lenght AS DOUBLE) AS product_description_lenght,
                TRY_CAST(product_photos_qty AS DOUBLE) AS product_photos_qty,
                TRY_CAST(product_weight_g AS DOUBLE) AS product_weight_g,
                TRY_CAST(product_length_cm AS DOUBLE) AS product_length_cm,
                TRY_CAST(product_height_cm AS DOUBLE) AS product_height_cm,
                TRY_CAST(product_width_cm AS DOUBLE) AS product_width_cm
            FROM bronze_products
            WHERE product_id IS NOT NULL
            """,
        )

        # Clean seller reference data and standardize location text.
        create_silver_table(
            conn,
            "silver_sellers",
            """
            SELECT DISTINCT
                seller_id,
                TRY_CAST(seller_zip_code_prefix AS INTEGER) AS seller_zip_code_prefix,
                LOWER(TRIM(seller_city)) AS seller_city,
                UPPER(TRIM(seller_state)) AS seller_state
            FROM bronze_sellers
            WHERE seller_id IS NOT NULL
            """,
        )

        # Clean payment records and standardize payment type values.
        create_silver_table(
            conn,
            "silver_payments",
            """
            SELECT DISTINCT
                order_id,
                TRY_CAST(payment_sequential AS INTEGER) AS payment_sequential,
                LOWER(TRIM(payment_type)) AS payment_type,
                TRY_CAST(payment_installments AS INTEGER) AS payment_installments,
                TRY_CAST(payment_value AS DOUBLE) AS payment_value
            FROM bronze_payments
            WHERE order_id IS NOT NULL
            """,
        )

        # Clean review rows and keep one latest row per review_id.
        create_silver_table(
            conn,
            "silver_reviews",
            """
            WITH ranked_reviews AS (
                SELECT
                    review_id,
                    order_id,
                    TRY_CAST(review_score AS INTEGER) AS review_score,
                    review_comment_title,
                    review_comment_message,
                    TRY_CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
                    TRY_CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY review_id
                        ORDER BY
                            TRY_CAST(review_answer_timestamp AS TIMESTAMP) DESC NULLS LAST,
                            TRY_CAST(review_creation_date AS TIMESTAMP) DESC NULLS LAST
                    ) AS row_num
                FROM bronze_reviews
                WHERE review_id IS NOT NULL
                  AND order_id IS NOT NULL
            )
            SELECT
                review_id,
                order_id,
                review_score,
                review_comment_title,
                review_comment_message,
                review_creation_date,
                review_answer_timestamp
            FROM ranked_reviews
            WHERE row_num = 1
            """,
        )
    finally:
        conn.close()
        print("DuckDB connection closed.")


if __name__ == "__main__":
    main()
