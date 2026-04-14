CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_events
(
    event_id String,
    event_type String,
    user_id String,
    session_id String,
    product_id String,
    page String,
    event_time DateTime,
    price Float64,
    quantity Int32,
    source String
)
ENGINE = MergeTree
ORDER BY (event_time, event_id);

CREATE TABLE IF NOT EXISTS analytics.event_metrics_5min
(
    window_start DateTime,
    window_end DateTime,
    event_type String,
    active_users UInt64,
    total_events UInt64
)
ENGINE = MergeTree
ORDER BY (window_start, event_type);

CREATE TABLE IF NOT EXISTS analytics.product_metrics_1hour
(
    window_start DateTime,
    window_end DateTime,
    product_id String,
    purchases UInt64,
    revenue Float64
)
ENGINE = MergeTree
ORDER BY (window_start,product_id);
