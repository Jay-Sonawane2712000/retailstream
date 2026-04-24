from pathlib import Path

import duckdb


DB_PATH = Path("data/retailstream.duckdb")
FILE_TABLE_MAPPINGS = [
    ("olist_orders_dataset.csv", "bronze_orders"),
    ("olist_order_items_dataset.csv", "bronze_order_items"),
    ("olist_customers_dataset.csv", "bronze_customers"),
    ("olist_products_dataset.csv", "bronze_products"),
    ("olist_sellers_dataset.csv", "bronze_sellers"),
    ("olist_order_payments_dataset.csv", "bronze_payments"),
    ("olist_order_reviews_dataset.csv", "bronze_reviews"),
]


def load_table(conn: duckdb.DuckDBPyConnection, source_file: str, table_name: str) -> None:
    source_path = Path("data/raw") / source_file

    # Clear the target table before each load.
    conn.execute(f"DELETE FROM {table_name}")

    # Remove prior metadata rows for this table so each run records the latest load cleanly.
    conn.execute("DELETE FROM bronze_metadata WHERE table_name = ?", [table_name])

    # Load raw CSV data directly into the Bronze table using DuckDB SQL.
    conn.execute(
        f"""
        INSERT INTO {table_name}
        SELECT *
        FROM read_csv_auto('{source_path.as_posix()}')
        """
    )

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]

    # Record load metadata after the table has been populated.
    conn.execute(
        """
        INSERT INTO bronze_metadata (
            table_name,
            row_count,
            load_timestamp,
            source_file
        )
        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
        """,
        [table_name, row_count, source_file],
    )

    print(f"Loaded {table_name}: {row_count} rows")


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))

    try:
        for source_file, table_name in FILE_TABLE_MAPPINGS:
            load_table(conn, source_file, table_name)
    finally:
        conn.close()
        print("DuckDB connection closed.")


if __name__ == "__main__":
    main()
