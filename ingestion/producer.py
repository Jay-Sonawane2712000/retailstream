import csv
import json
from pathlib import Path

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "retail_orders"
SOURCE_FILE = Path("data/raw/olist_orders_dataset.csv")
MAX_ROWS = 100


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed for key {msg.key().decode('utf-8')}: {err}")


def main() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    with SOURCE_FILE.open(mode="r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        for row_number, row in enumerate(reader, start=1):
            if row_number > MAX_ROWS:
                break

            order_id = row["order_id"]
            message_value = json.dumps(row)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=order_id,
                value=message_value,
                callback=delivery_report,
            )

            producer.poll(0)
            print(f"Sent order_id: {order_id}")

    producer.flush()
    print(f"Finished sending {MAX_ROWS} rows to topic: {KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
