import json
import sys
from pathlib import Path

import duckdb


DB_PATH = Path("data/retailstream.duckdb")
REPORT_PATH = Path("quality/quality_report.json")


def run_check(conn: duckdb.DuckDBPyConnection, query: str) -> bool:
    result = conn.execute(query).fetchone()[0]
    return bool(result)


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))
    results = {}

    checks = {
        "bronze_orders_row_count_gt_0": """
            SELECT COUNT(*) > 0
            FROM bronze_orders
        """,
        "fact_orders_row_count_gt_0": """
            SELECT COUNT(*) > 0
            FROM fact_orders
        """,
        "fact_orders_customer_id_no_nulls": """
            SELECT COUNT(*) = 0
            FROM fact_orders
            WHERE customer_id IS NULL
        """,
        "fact_orders_product_id_no_nulls": """
            SELECT COUNT(*) = 0
            FROM fact_orders
            WHERE product_id IS NULL
        """,
        "fact_orders_total_revenue_no_nulls": """
            SELECT COUNT(*) = 0
            FROM fact_orders
            WHERE total_revenue IS NULL
        """,
        "dim_customers_customer_id_unique": """
            SELECT COUNT(*) = 0
            FROM (
                SELECT customer_id
                FROM dim_customers
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            )
        """,
        "dim_products_product_id_unique": """
            SELECT COUNT(*) = 0
            FROM (
                SELECT product_id
                FROM dim_products
                GROUP BY product_id
                HAVING COUNT(*) > 1
            )
        """,
        "fact_orders_customer_id_in_dim_customers": """
            SELECT COUNT(*) = 0
            FROM fact_orders AS f
            LEFT JOIN dim_customers AS d
                ON f.customer_id = d.customer_id
            WHERE d.customer_id IS NULL
        """,
        "fact_orders_product_id_in_dim_products": """
            SELECT COUNT(*) = 0
            FROM fact_orders AS f
            LEFT JOIN dim_products AS d
                ON f.product_id = d.product_id
            WHERE d.product_id IS NULL
        """,
        "fact_orders_total_revenue_gte_0": """
            SELECT COUNT(*) = 0
            FROM fact_orders
            WHERE total_revenue < 0
        """,
    }

    try:
        for check_name, query in checks.items():
            passed = run_check(conn, query)
            results[check_name] = "PASS" if passed else "FAIL"
    finally:
        conn.close()

    REPORT_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")

    total_checks = len(results)
    passed_checks = sum(1 for status in results.values() if status == "PASS")
    failed_checks = total_checks - passed_checks

    print(f"TOTAL CHECKS: {total_checks}")
    print(f"PASSED: {passed_checks}")
    print(f"FAILED: {failed_checks}")

    if failed_checks > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
