import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer_events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "customer-analytics-consumer")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics_pass")
FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "5"))
CLICKHOUSE_CONNECT_TIMEOUT_SECONDS = int(os.getenv("CLICKHOUSE_CONNECT_TIMEOUT_SECONDS", "90"))
CLICKHOUSE_RETRY_INTERVAL_SECONDS = int(os.getenv("CLICKHOUSE_RETRY_INTERVAL_SECONDS", "3"))


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )


def wait_for_clickhouse():
    deadline = time.time() + CLICKHOUSE_CONNECT_TIMEOUT_SECONDS
    last_error = None

    while time.time() < deadline:
        try:
            client = get_clickhouse_client()
            client.command("SELECT 1")
            print("Connected to ClickHouse.")
            return client
        except OperationalError as exc:
            last_error = exc
            print("Waiting for ClickHouse to become ready...")
            time.sleep(CLICKHOUSE_RETRY_INTERVAL_SECONDS)

    raise RuntimeError(
        f"ClickHouse did not become ready within {CLICKHOUSE_CONNECT_TIMEOUT_SECONDS} seconds"
    ) from last_error


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(3, 9, 0),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000
    )


def parse_event_time(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def floor_time(ts, window_minutes):
    discard = timedelta(
        minutes=ts.minute % window_minutes,
        seconds=ts.second,
        microseconds=ts.microsecond
    )
    return ts - discard


def upsert_event_metrics(client, metrics):
    if not metrics:
        return

    rows = []
    delete_conditions = []

    for (window_start, event_type), values in metrics.items():
        window_end = window_start + timedelta(minutes=5)
        active_users = len(values["users"])
        total_events = values["total_events"]
        rows.append((window_start, window_end, event_type, active_users, total_events))
        delete_conditions.append(
            (
                f"(window_start = toDateTime('{window_start:%Y-%m-%d %H:%M:%S}') "
                f"AND window_end = toDateTime('{window_end:%Y-%m-%d %H:%M:%S}') "
                f"AND event_type = '{event_type}')"
            )
        )

    client.command(
        "ALTER TABLE event_metrics_5min DELETE WHERE " + " OR ".join(delete_conditions)
    )
    client.insert(
        "event_metrics_5min",
        rows,
        column_names=[
            "window_start",
            "window_end",
            "event_type",
            "active_users",
            "total_events"
        ]
    )
    print(f"Upserted {len(rows)} rows into analytics.event_metrics_5min")


def upsert_product_metrics(client, metrics):
    if not metrics:
        return

    rows = []
    delete_conditions = []

    for (window_start, product_id), values in metrics.items():
        window_end = window_start + timedelta(hours=1)
        purchases = values["purchases"]
        revenue = round(values["revenue"], 2)
        rows.append((window_start, window_end, product_id, purchases, revenue))
        delete_conditions.append(
            (
                f"(window_start = toDateTime('{window_start:%Y-%m-%d %H:%M:%S}') "
                f"AND window_end = toDateTime('{window_end:%Y-%m-%d %H:%M:%S}') "
                f"AND product_id = '{product_id}')"
            )
        )

    client.command(
        "ALTER TABLE product_metrics_1hour DELETE WHERE " + " OR ".join(delete_conditions)
    )
    client.insert(
        "product_metrics_1hour",
        rows,
        column_names=[
            "window_start",
            "window_end",
            "product_id",
            "purchases",
            "revenue"
        ]
    )
    print(f"Upserted {len(rows)} rows into analytics.product_metrics_1hour")


def flush_batch(client, raw_events, event_metrics, product_metrics):
    if not raw_events:
        return

    client.insert(
        "raw_events",
        raw_events,
        column_names=[
            "event_id",
            "event_type",
            "user_id",
            "session_id",
            "product_id",
            "page",
            "event_time",
            "price",
            "quantity",
            "source"
        ]
    )
    print(f"Inserted {len(raw_events)} rows into analytics.raw_events")

    upsert_event_metrics(client, event_metrics)
    upsert_product_metrics(client, product_metrics)


def main():
    consumer = create_consumer()
    client = wait_for_clickhouse()
    seen_event_ids = set()
    raw_events = []
    event_metrics = defaultdict(lambda: {"users": set(), "total_events": 0})
    product_metrics = defaultdict(lambda: {"purchases": 0, "revenue": 0.0})
    last_flush = time.time()

    print("Consuming Kafka events and writing analytics to ClickHouse...")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    event = message.value
                    event_id = event.get("event_id")
                    event_time_raw = event.get("event_time")

                    if not event_id or not event_time_raw or event_id in seen_event_ids:
                        continue

                    event_time = parse_event_time(event_time_raw)
                    seen_event_ids.add(event_id)
                    raw_events.append(
                        (
                            event_id,
                            event.get("event_type"),
                            event.get("user_id"),
                            event.get("session_id"),
                            event.get("product_id"),
                            event.get("page"),
                            event_time,
                            float(event.get("price", 0.0) or 0.0),
                            int(event.get("quantity", 0) or 0),
                            event.get("source")
                        )
                    )

                    metric_key = (floor_time(event_time, 5), event.get("event_type"))
                    event_metrics[metric_key]["users"].add(event.get("user_id"))
                    event_metrics[metric_key]["total_events"] += 1

                    if event.get("event_type") == "purchase":
                        product_key = (
                            floor_time(event_time.replace(minute=0, second=0, microsecond=0), 60),
                            event.get("product_id")
                        )
                        product_metrics[product_key]["purchases"] += 1
                        product_metrics[product_key]["revenue"] += (
                            float(event.get("price", 0.0) or 0.0) *
                            int(event.get("quantity", 0) or 0)
                        )

            if raw_events and (time.time() - last_flush >= FLUSH_INTERVAL_SECONDS):
                flush_batch(client, raw_events, event_metrics, product_metrics)
                raw_events.clear()
                event_metrics.clear()
                product_metrics.clear()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nStreaming job stopped by user.")
    finally:
        if raw_events:
            flush_batch(client, raw_events, event_metrics, product_metrics)
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
