from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    countDistinct,
    count,
    sum as spark_sum,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
import clickhouse_connect

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("source", StringType(), True)
])

spark = SparkSession.builder \
    .appName("CustomerActivityAnalytics") \
    .master("local[2]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("event_time").isNotNull()) \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["event_id"])

metrics_5min = parsed_df.groupBy(
    window(col("event_time"), "5 minutes"),
    col("event_type")
).agg(
    countDistinct("user_id").alias("active_users"),
    count("*").alias("total_events")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("event_type"),
    col("active_users"),
    col("total_events")
)

product_metrics_1hour = parsed_df.filter(
    col("event_type") == "purchase"
).groupBy(
    window(col("event_time"), "1 hour"),
    col("product_id")
).agg(
    count("*").alias("purchases"),
    spark_sum(
        coalesce(col("price"), lit(0.0)) * coalesce(col("quantity"), lit(0))
    ).alias("revenue")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("product_id"),
    col("purchases"),
    col("revenue")
)


def write_metrics_5min(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    client = clickhouse_connect.get_client(host="localhost", port=8123)
    try:
        data = [tuple(row) for row in rows]
        client.insert(
            "analytics.event_metrics_5min",
            data,
            column_names=[
                "window_start",
                "window_end",
                "event_type",
                "active_users",
                "total_events"
            ]
        )
        print(f"Inserted {len(data)} rows into analytics.event_metrics_5min")
    finally:
        client.close()


def write_product_metrics(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    client = clickhouse_connect.get_client(host="localhost", port=8123)
    try:
        data = [tuple(row) for row in rows]
        client.insert(
            "analytics.product_metrics_1hour",
            data,
            column_names=[
                "window_start",
                "window_end",
                "product_id",
                "purchases",
                "revenue"
            ]
        )
        print(f"Inserted {len(data)} rows into analytics.product_metrics_1hour")
    finally:
        client.close()


query1 = metrics_5min.writeStream \
    .outputMode("update") \
    .foreachBatch(write_metrics_5min) \
    .option("checkpointLocation", "checkpoints/metrics_5min") \
    .start()

query2 = product_metrics_1hour.writeStream \
    .outputMode("update") \
    .foreachBatch(write_product_metrics) \
    .option("checkpointLocation", "checkpoints/product_metrics_1hour") \
    .start()

spark.streams.awaitAnyTermination()