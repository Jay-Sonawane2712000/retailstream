from pathlib import Path

import duckdb


# DuckDB database file for the RetailStream project.
DB_PATH = Path("data/retailstream.duckdb")


def main() -> None:
    # Ensure the data directory exists before connecting.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB. The database file is created if it does not exist.
    conn = duckdb.connect(str(DB_PATH))

    try:
        # bronze_orders stores the raw order-level records.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_orders (
                order_id VARCHAR,
                customer_id VARCHAR,
                order_status VARCHAR,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            )
            """
        )

        # bronze_order_items stores line-item level order details.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_order_items (
                order_id VARCHAR,
                order_item_id INTEGER,
                product_id VARCHAR,
                seller_id VARCHAR,
                shipping_limit_date TIMESTAMP,
                price DOUBLE,
                freight_value DOUBLE
            )
            """
        )

        # bronze_customers stores customer reference and location fields.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_customers (
                customer_id VARCHAR,
                customer_unique_id VARCHAR,
                customer_zip_code_prefix INTEGER,
                customer_city VARCHAR,
                customer_state VARCHAR
            )
            """
        )

        # bronze_products stores product metadata and physical dimensions.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_products (
                product_id VARCHAR,
                product_category_name VARCHAR,
                product_name_lenght DOUBLE,
                product_description_lenght DOUBLE,
                product_photos_qty DOUBLE,
                product_weight_g DOUBLE,
                product_length_cm DOUBLE,
                product_height_cm DOUBLE,
                product_width_cm DOUBLE
            )
            """
        )

        # bronze_sellers stores seller reference and location fields.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_sellers (
                seller_id VARCHAR,
                seller_zip_code_prefix INTEGER,
                seller_city VARCHAR,
                seller_state VARCHAR
            )
            """
        )

        # bronze_payments stores payment method and payment amount details.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_payments (
                order_id VARCHAR,
                payment_sequential INTEGER,
                payment_type VARCHAR,
                payment_installments INTEGER,
                payment_value DOUBLE
            )
            """
        )

        # bronze_reviews stores order review scores, text, and timestamps.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_reviews (
                review_id VARCHAR,
                order_id VARCHAR,
                review_score INTEGER,
                review_comment_title VARCHAR,
                review_comment_message VARCHAR,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP
            )
            """
        )

        # bronze_metadata stores load-level metadata for Bronze tables.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_metadata (
                table_name VARCHAR,
                row_count INTEGER,
                load_timestamp TIMESTAMP,
                source_file VARCHAR
            )
            """
        )

        print(f"Bronze tables created successfully in: {DB_PATH}")
    finally:
        conn.close()
        print("DuckDB connection closed.")


if __name__ == "__main__":
    main()
