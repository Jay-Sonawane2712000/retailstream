from pathlib import Path

import duckdb


DB_PATH = Path("data/retailstream.duckdb")


def print_query_results(conn: duckdb.DuckDBPyConnection, title: str, query: str) -> None:
    print("=" * 80)
    print(title)
    print("=" * 80)

    results = conn.execute(query).fetchdf()
    print(results.to_string(index=False))
    print()


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))

    try:
        print_query_results(
            conn,
            "1. Total Revenue",
            """
            SELECT SUM(payment_value) AS total_revenue
            FROM fact_orders
            """,
        )

        print_query_results(
            conn,
            "2. Orders per Month",
            """
            SELECT
                d.year,
                d.month,
                COUNT(DISTINCT f.order_id) AS orders_count
            FROM fact_orders AS f
            INNER JOIN dim_date AS d
                ON f.order_purchase_timestamp = d.order_purchase_timestamp
            GROUP BY d.year, d.month
            ORDER BY d.year, d.month
            """,
        )

        print_query_results(
            conn,
            "3. Top 10 Products by Revenue",
            """
            SELECT
                product_id,
                SUM(price) AS total_product_revenue
            FROM fact_orders
            GROUP BY product_id
            ORDER BY total_product_revenue DESC
            LIMIT 10
            """,
        )

        print_query_results(
            conn,
            "4. Top States by Orders",
            """
            SELECT
                c.customer_state,
                COUNT(DISTINCT f.order_id) AS orders_count
            FROM fact_orders AS f
            INNER JOIN dim_customers AS c
                ON f.customer_id = c.customer_id
            GROUP BY c.customer_state
            ORDER BY orders_count DESC
            """,
        )

        print_query_results(
            conn,
            "5. Average Order Value",
            """
            SELECT AVG(payment_value) AS average_order_value
            FROM fact_orders
            """,
        )
    finally:
        conn.close()
        print("DuckDB connection closed.")


if __name__ == "__main__":
    main()
