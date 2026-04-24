from pathlib import Path

import pandas as pd


RAW_DIR = Path("data/raw")
SAMPLE_DIR = Path("data/sample")
ROW_LIMIT = 200

FILE_MAPPINGS = {
    "olist_orders_dataset.csv": "olist_orders_sample.csv",
    "olist_order_items_dataset.csv": "olist_order_items_sample.csv",
    "olist_customers_dataset.csv": "olist_customers_sample.csv",
}


def main() -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for source_name, output_name in FILE_MAPPINGS.items():
        source_path = RAW_DIR / source_name
        output_path = SAMPLE_DIR / output_name

        df = pd.read_csv(source_path).head(ROW_LIMIT)
        df.to_csv(output_path, index=False)

        print(f"Created: {output_path}")


if __name__ == "__main__":
    main()
