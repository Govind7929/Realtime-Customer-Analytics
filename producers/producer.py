import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "customer_events"

EVENT_TYPES = ["page_view", "add_to_cart", "checkout", "purchase"]
PAGES = ["home", "search", "product", "cart", "checkout"]
PRODUCTS = ["P101", "P102", "P103", "P104", "P105"]
SOURCES = ["web", "mobile"]


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            api_version=(3, 9, 0),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        return producer
    except Exception as e:
        print("Failed to connect to Kafka:", e)
        raise


def generate_event():
    event_type = random.choices(
        EVENT_TYPES,
        weights=[50, 25, 15, 10],
        k=1
    )[0]

    price = round(random.uniform(100, 5000), 2)
    quantity = random.randint(1, 3)

    if event_type != "purchase":
        price = 0.0
        quantity = 0

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": f"U{random.randint(1000, 9999)}",
        "session_id": str(uuid.uuid4()),
        "product_id": random.choice(PRODUCTS),
        "page": random.choice(PAGES),
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "price": price,
        "quantity": quantity,
        "source": random.choice(SOURCES)
    }


def main():
    producer = None
    try:
        producer = create_producer()
        print("Sending events to Kafka topic... Press Ctrl+C to stop.")

        while True:
            event = generate_event()
            future = producer.send(TOPIC, value=event)
            record_metadata = future.get(timeout=10)

            print(
                f"Sent to topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset} -> {event}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    except KafkaError as e:
        print("Kafka error:", e)

    except Exception as e:
        print("Unexpected error:", e)

    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
