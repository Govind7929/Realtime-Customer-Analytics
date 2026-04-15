import json
import os

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer_events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "customer-events-printer")


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(3, 9, 0),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print(f"Listening to Kafka topic '{KAFKA_TOPIC}'...")

    try:
        for message in consumer:
            print(message.value)
    except KeyboardInterrupt:
        print("\nKafka stream printer stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
