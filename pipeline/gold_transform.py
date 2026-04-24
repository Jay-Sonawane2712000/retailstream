from pathlib import Path

import duckdb


DB_PATH = Path("data/retailstream.duckdb")


def create_gold_table(
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
        # Customer dimension with one row per customer.
        create_gold_table(
            conn,
            "dim_customers",
            """
            SELECT DISTINCT
                customer_id,
                customer_unique_id,
                customer_city,
                customer_state
            FROM silver_customers
            WHERE customer_id IS NOT NULL
            """,
        )

        # Product dimension with one row per product.
        create_gold_table(
            conn,
            "dim_products",
            """
            SELECT DISTINCT
                product_id,
                product_category_name
            FROM silver_products
            WHERE product_id IS NOT NULL
            """,
        )

        # Seller dimension with one row per seller.
        create_gold_table(
            conn,
            "dim_sellers",
            """
            SELECT DISTINCT
                seller_id,
                seller_city,
                seller_state
            FROM silver_sellers
            WHERE seller_id IS NOT NULL
            """,
        )

        # Date dimension derived from order purchase timestamps.
        create_gold_table(
            conn,
            "dim_date",
            """
            SELECT DISTINCT
                order_purchase_timestamp,
                YEAR(order_purchase_timestamp) AS year,
                MONTH(order_purchase_timestamp) AS month,
                DAY(order_purchase_timestamp) AS day,
                DAYOFWEEK(order_purchase_timestamp) AS weekday
            FROM silver_orders
            WHERE order_purchase_timestamp IS NOT NULL
            """,
        )

        # Fact table combining orders, items, and payments.
        create_gold_table(
            conn,
            "fact_orders",
            """
            SELECT DISTINCT
                o.order_id,
                o.customer_id,
                o.order_status,
                o.order_purchase_timestamp,
                i.product_id,
                i.seller_id,
                i.price,
                i.freight_value,
                p.payment_value
            FROM silver_orders AS o
            INNER JOIN silver_order_items AS i
                ON o.order_id = i.order_id
            LEFT JOIN silver_payments AS p
                ON o.order_id = p.order_id
            WHERE o.order_id IS NOT NULL
              AND o.customer_id IS NOT NULL
              AND i.product_id IS NOT NULL
              AND i.seller_id IS NOT NULL
            """,
        )
    finally:
        conn.close()
        print("DuckDB connection closed.")


if __name__ == "__main__":
    main()
