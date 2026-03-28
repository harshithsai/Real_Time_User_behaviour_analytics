# spark_streaming/stream_processor.py
# PySpark Structured Streaming job
# Reads from Kafka, writes to Snowflake RAW_CLICKS, RAW_SESSIONS, RAW_PURCHASES

import os
import sys
import pathlib

# ── Windows Hadoop fix — MUST be before any PySpark import ────
if sys.platform == "win32":
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, approx_count_distinct,
    avg, max as spark_max, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType
)
from dotenv import load_dotenv

load_dotenv()

# ── Checkpoint paths ───────────────────────────────────────────
CHECKPOINT_CLICKS    = "file:///C:/tmp/checkpoints/clicks"
CHECKPOINT_SESSIONS  = "file:///C:/tmp/checkpoints/sessions"
CHECKPOINT_PURCHASES = "file:///C:/tmp/checkpoints/purchases"
CHECKPOINT_AGG       = "file:///C:/tmp/checkpoints/aggregates"

# ── Timestamp format matching event_generator.py ──────────────
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"


# ── Spark Session ──────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("UserBehaviorStreamProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "net.snowflake:snowflake-jdbc:3.13.30,"
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ── Schemas — match Snowflake columns exactly (no INGESTED_AT) ─

# RAW_CLICKS: event_id, event_type, user_id, session_id, product_id,
#             product_name, product_category, page, device, country,
#             referrer, price_seen, event_timestamp
CLICK_SCHEMA = StructType([
    StructField("event_id",         StringType(), True),
    StructField("event_type",       StringType(), True),
    StructField("user_id",          StringType(), True),
    StructField("session_id",       StringType(), True),
    StructField("product_id",       StringType(), True),
    StructField("product_name",     StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("page",             StringType(), True),
    StructField("device",           StringType(), True),
    StructField("country",          StringType(), True),
    StructField("referrer",         StringType(), True),
    StructField("price_seen",       DoubleType(), True),
    StructField("event_timestamp",  StringType(), True),
])

# RAW_SESSIONS: event_id, session_id, user_id, session_start,
#               duration_seconds, pages_visited, device, country,
#               is_new_user, event_timestamp
SESSION_SCHEMA = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("session_id",       StringType(),  True),
    StructField("user_id",          StringType(),  True),
    StructField("session_start",    StringType(),  True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("pages_visited",    IntegerType(), True),
    StructField("device",           StringType(),  True),
    StructField("country",          StringType(),  True),
    StructField("is_new_user",      BooleanType(), True),
    StructField("event_timestamp",  StringType(),  True),
])

# RAW_PURCHASES: event_id, order_id, user_id, session_id, product_id,
#                product_name, product_category, quantity, unit_price,
#                total_amount, payment_method, country, is_fraudulent,
#                event_timestamp
PURCHASE_SCHEMA = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("order_id",         StringType(),  True),
    StructField("user_id",          StringType(),  True),
    StructField("session_id",       StringType(),  True),
    StructField("product_id",       StringType(),  True),
    StructField("product_name",     StringType(),  True),
    StructField("product_category", StringType(),  True),
    StructField("quantity",         IntegerType(), True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("total_amount",     DoubleType(),  True),
    StructField("payment_method",   StringType(),  True),
    StructField("country",          StringType(),  True),
    StructField("is_fraudulent",    BooleanType(), True),
    StructField("event_timestamp",  StringType(),  True),
])


# ── Snowflake Config ───────────────────────────────────────────
SNOWFLAKE_OPTIONS = {
    "sfURL":       f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser":      os.getenv("SNOWFLAKE_USER"),
    "sfPassword":  os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase":  "RAW_DB",
    "sfSchema":    "EVENTS",
    "sfWarehouse": "ANALYTICS_WH",
}


def write_to_snowflake(batch_df, batch_id: int, table_name: str):
    """Write a micro-batch to Snowflake. Skips empty batches."""
    row_count = batch_df.count()
    if row_count > 0:
        (
            batch_df.write
            .format("snowflake")
            .options(**SNOWFLAKE_OPTIONS)
            .option("dbtable", table_name)
            .mode("append")
            .save()
        )
        print(f"[Batch {batch_id}] ✅ Wrote {row_count:,} rows → {table_name}")
    else:
        print(f"[Batch {batch_id}] ⏭ Empty batch, skipping {table_name}")


# ── Main Streaming Job ─────────────────────────────────────────
def run_streaming_job():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # ── Helper: read a Kafka topic ───────────────────────────
    def read_kafka(topic):
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    # ── Parse clicks ─────────────────────────────────────────
    clicks = (
        read_kafka("user-clicks")
        .select(from_json(col("value").cast("string"), CLICK_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp",
                    to_timestamp(col("event_timestamp"), TS_FORMAT))
        .withWatermark("event_timestamp", "10 minutes")
    )

    # ── Parse sessions ───────────────────────────────────────
    sessions = (
        read_kafka("user-sessions")
        .select(from_json(col("value").cast("string"), SESSION_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp",
                    to_timestamp(col("event_timestamp"), TS_FORMAT))
        .withColumn("session_start",
                    to_timestamp(col("session_start"), TS_FORMAT))
        .withWatermark("event_timestamp", "10 minutes")
    )

    # ── Parse purchases ──────────────────────────────────────
    purchases = (
        read_kafka("user-purchases")
        .select(from_json(col("value").cast("string"), PURCHASE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp",
                    to_timestamp(col("event_timestamp"), TS_FORMAT))
        .withWatermark("event_timestamp", "10 minutes")
    )

    # ── Windowed aggregation on clicks ───────────────────────
    click_aggregates = (
        clicks
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("product_category"),
            col("device"),
            col("country")
        )
        .agg(
            count("event_id").alias("click_count"),
            approx_count_distinct(col("user_id")).alias("unique_users"),
            avg("price_seen").alias("avg_price_seen"),
            spark_max("price_seen").alias("max_price_seen")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_category"),
            col("device"),
            col("country"),
            col("click_count"),
            col("unique_users"),
            col("avg_price_seen"),
            col("max_price_seen"),
            current_timestamp().alias("processed_at")
        )
    )

    # ── Stream 1: clicks → Snowflake RAW_CLICKS ──────────────
    clicks_query = (
        clicks.writeStream
        .foreachBatch(lambda df, id: write_to_snowflake(df, id, "RAW_CLICKS"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", CHECKPOINT_CLICKS)
        .start()
    )

    # ── Stream 2: sessions → Snowflake RAW_SESSIONS ──────────
    sessions_query = (
        sessions.writeStream
        .foreachBatch(lambda df, id: write_to_snowflake(df, id, "RAW_SESSIONS"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", CHECKPOINT_SESSIONS)
        .start()
    )

    # ── Stream 3: purchases → Snowflake RAW_PURCHASES ────────
    purchases_query = (
        purchases.writeStream
        .foreachBatch(lambda df, id: write_to_snowflake(df, id, "RAW_PURCHASES"))
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", CHECKPOINT_PURCHASES)
        .start()
    )

    # ── Stream 4: aggregates → console ───────────────────────
    agg_query = (
        click_aggregates.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", CHECKPOINT_AGG)
        .start()
    )

    print("🔥 PySpark Streaming job started.")
    print("   → RAW_CLICKS    ← user-clicks")
    print("   → RAW_SESSIONS  ← user-sessions")
    print("   → RAW_PURCHASES ← user-purchases")
    print("   → Console aggregates every 60s")
    print("Press Ctrl+C to stop\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    for path in [
        "C:/tmp/checkpoints/clicks",
        "C:/tmp/checkpoints/sessions",
        "C:/tmp/checkpoints/purchases",
        "C:/tmp/checkpoints/aggregates",
    ]:
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    run_streaming_job()