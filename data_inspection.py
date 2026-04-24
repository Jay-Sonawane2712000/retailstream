from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/raw")
FILES_TO_INSPECT = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
]


def inspect_csv(file_name: str) -> None:
    file_path = DATA_DIR / file_name

    print("=" * 100)
    print(f"Dataset: {file_name}")
    print(f"Path: {file_path}")
    print("=" * 100)

    df = pd.read_csv(file_path)

    print("\nFirst 5 rows:")
    print(df.head())

    print("\nColumn names:")
    print(df.columns.tolist())

    print("\nData types:")
    print(df.dtypes)

    print("\nShape:")
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    print("\nNull values per column:")
    print(df.isnull().sum())
    print("\n")


def main() -> None:
    for file_name in FILES_TO_INSPECT:
        inspect_csv(file_name)


if __name__ == "__main__":
    main()
