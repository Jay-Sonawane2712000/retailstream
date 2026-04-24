import json
from pathlib import Path

import duckdb
from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "retail_orders"
KAFKA_GROUP_ID = "retailstream-orders-consumer"
DB_PATH = Path("data/retailstream.duckdb")
MAX_MESSAGES = 100


def clean_row(row: dict) -> dict:
    # Convert empty strings from Kafka messages into None so DuckDB can store NULLs.
    return {key: (None if value == "" else value) for key, value in row.items()}


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    conn = duckdb.connect(str(DB_PATH))
    consumer.subscribe([KAFKA_TOPIC])

    insert_sql = """
        INSERT INTO bronze_orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    messages_read = 0

    try:
        while messages_read < MAX_MESSAGES:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            row = json.loads(msg.value().decode("utf-8"))
            row = clean_row(row)

            conn.execute(
                insert_sql,
                [
                    row.get("order_id"),
                    row.get("customer_id"),
                    row.get("order_status"),
                    row.get("order_purchase_timestamp"),
                    row.get("order_approved_at"),
                    row.get("order_delivered_carrier_date"),
                    row.get("order_delivered_customer_date"),
                    row.get("order_estimated_delivery_date"),
                ],
            )

            messages_read += 1
            print(f"Inserted order_id: {row.get('order_id')}")
    finally:
        consumer.close()
        conn.close()
        print("Kafka consumer closed.")
        print("DuckDB connection closed.")


if __name__ == "__main__":
    main()
