#!/usr/bin/env python3
"""
Spark Structured Streaming Consumer for AKI Pipeline (Phase 9).
Ingests real-time synthetic data from Kafka and applies streaming algorithms:
- Bloom Filter (Deduplication)
- Flajolet-Martin / HyperLogLog (Approximate Distinct Counting)
- DGIM-style Windowing (Memory-efficient time-window aggregation)
- Reservoir Sampling (Live random sampling)
"""

import sys
import json
import random
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, approx_count_distinct, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# Ensure project root is importable when run from subdirectories.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from scripts.sf_env import snowflake_spark_options

# --- Streaming Algorithm Implementations (for foreachBatch) ---

class SimpleBloomFilter:
    """Basic Bloom Filter simulation for streaming deduplication."""
    def __init__(self, size=10000):
        self.size = size
        self.bit_array = [0] * size

    def _hash(self, item):
        return hash(item) % self.size

    def add(self, item):
        self.bit_array[self._hash(item)] = 1

    def __contains__(self, item):
        return self.bit_array[self._hash(item)] == 1

class ReservoirSampler:
    """Reservoir Sampling simulation for unbiased stream sampling."""
    def __init__(self, k=5):
        self.k = k
        self.reservoir = []
        self.count = 0

    def add(self, item):
        self.count += 1
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
        else:
            j = random.randint(0, self.count - 1)
            if j < self.k:
                self.reservoir[j] = item

# Global algorithm state for the demo
bloom_filter = SimpleBloomFilter()
sampler = ReservoirSampler(k=3)
snowflake_options = {}
METRICS_SCHEMA = StructType(
    [
        StructField("batch_id", LongType(), False),
        StructField("metric_ts", TimestampType(), False),
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("event_type", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("extra_json", StringType(), True),
    ]
)


def _write_metrics_to_snowflake(batch_df):
    if not snowflake_options or batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("snowflake")
        .options(**snowflake_options)
        .option(
            "preactions",
            """
            CREATE TABLE IF NOT EXISTS MART_LIVE_STREAM_METRICS (
              BATCH_ID NUMBER,
              METRIC_TS TIMESTAMP_NTZ,
              WINDOW_START TIMESTAMP_NTZ,
              WINDOW_END TIMESTAMP_NTZ,
              EVENT_TYPE STRING,
              METRIC_NAME STRING,
              METRIC_VALUE FLOAT,
              EXTRA_JSON STRING
            )
            """,
        )
        .option("dbtable", "MART_LIVE_STREAM_METRICS")
        .mode("append")
        .save()
    )


def process_microbatch(batch_df, batch_id):
    """
    Applied to every micro-batch of data arriving from Kafka.
    """
    print(f"\n--- Processing Micro-Batch {batch_id} ---")
    
    # 1. Bloom Filter Deduplication
    # We collect the batch and filter duplicates using our Bloom Filter
    events = batch_df.collect()
    if not events:
        print("No events in this micro-batch; skipping Snowflake metric write.")
        return
    unique_events = []
    
    for row in events:
        event_str = f"{row.subject_id}_{row.charttime}_{row.event_type}"
        if event_str not in bloom_filter:
            bloom_filter.add(event_str)
            unique_events.append(row)
            
            # 4. Reservoir Sampling
            sampler.add(row.asDict())

    print(f"Bloom Filter: Processed {len(events)} events. Allowed {len(unique_events)} unique events.")
    print(f"Reservoir Sample (k=3): Currently holding {len(sampler.reservoir)} live random events.")
    for s in sampler.reservoir:
        print(f"  -> Sampled: Patient {s['subject_id']}, {s['event_type']} = {s['value']} {s['uom']}")

    metric_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    duplicate_rate = 0.0 if len(events) == 0 else float((len(events) - len(unique_events)) / len(events))
    sample_preview = [
        {
            "subject_id": int(s["subject_id"]) if s.get("subject_id") is not None else None,
            "event_type": s.get("event_type"),
            "value": float(s["value"]) if s.get("value") is not None else None,
            "uom": s.get("uom"),
        }
        for s in sampler.reservoir
    ]
    metrics_rows = [
        (int(batch_id), metric_ts, None, None, "all", "events_processed", float(len(events)), None),
        (
            int(batch_id),
            metric_ts,
            None,
            None,
            "all",
            "events_allowed_after_bloom",
            float(len(unique_events)),
            None,
        ),
        (
            int(batch_id),
            metric_ts,
            None,
            None,
            "all",
            "bloom_duplicate_rate",
            duplicate_rate,
            None,
        ),
        (
            int(batch_id),
            metric_ts,
            None,
            None,
            "all",
            "reservoir_size",
            float(len(sampler.reservoir)),
            json.dumps({"sample": sample_preview}),
        ),
    ]
    metrics_df = batch_df.sparkSession.createDataFrame(metrics_rows, schema=METRICS_SCHEMA)
    _write_metrics_to_snowflake(metrics_df)


def process_windowed_metrics(batch_df, batch_id):
    print(f"\n--- Writing Window Metrics for Micro-Batch {batch_id} ---")
    rows = batch_df.collect()
    if not rows:
        print("No window metrics to write in this batch.")
        return

    metric_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    metric_rows = []
    for row in rows:
        ws = row["window"]["start"]
        we = row["window"]["end"]
        event_type = row["event_type"]
        metric_rows.append(
            (
                int(batch_id),
                metric_ts,
                ws,
                we,
                event_type,
                "approx_unique_patients",
                float(row["approx_unique_patients"]),
                None,
            )
        )
        metric_rows.append(
            (
                int(batch_id),
                metric_ts,
                ws,
                we,
                event_type,
                "total_events_in_window",
                float(row["total_events"]),
                None,
            )
        )

    metrics_df = batch_df.sparkSession.createDataFrame(metric_rows, schema=METRICS_SCHEMA)
    _write_metrics_to_snowflake(metrics_df)
    print(f"Persisted {len(metric_rows)} window metric rows to Snowflake.")


def main():
    global snowflake_options
    spark = (
        SparkSession.builder.appName("aki-streaming-consumer")
        # Include Kafka SQL package
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2",
                    "net.snowflake:spark-snowflake_2.12:3.1.8",
                    "net.snowflake:snowflake-jdbc:3.15.1",
                ]
            ),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    snowflake_options = snowflake_spark_options()
    snowflake_options["sfSchema"] = "MART"

    # Define the JSON schema we are receiving from Kafka
    schema = StructType([
        StructField("subject_id", LongType(), True),
        StructField("hadm_id", LongType(), True),
        StructField("stay_id", LongType(), True),
        StructField("charttime", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("uom", StringType(), True)
    ])

    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "aki.live.events")
    print(f"Connecting to Kafka broker={kafka_broker}, topic={kafka_topic} ...")
    
    # Read from Kafka topic
    # Using rate stream for testing if Kafka is unavailable, but normally use "kafka" format
    try:
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_broker)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .load()
        )
        print("Successfully connected to Kafka.")
    except Exception as e:
        print(f"Warning: Could not connect to Kafka. {e}")
        print("Please ensure your kafka producer is running and writing to localhost:9092")
        sys.exit(1)

    # Parse JSON
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Cast charttime to timestamp for Spark Windowing
    parsed_stream = parsed_stream.withColumn("timestamp", col("charttime").cast("timestamp"))

    # --- Streaming Algorithms inside Spark SQL ---
    
    # 2. DGIM equivalent (Sliding Window Aggregation)
    # 3. Flajolet-Martin equivalent (approx_count_distinct / HyperLogLog)
    
    # We maintain a sliding 1-hour window, updating every 10 minutes.
    # Inside the window, we approximate distinct patients and count total events.
    windowed_metrics = (
        parsed_stream
        .withWatermark("timestamp", "1 hour")
        .groupBy(
            window(col("timestamp"), "1 hour", "10 minutes"),
            col("event_type")
        )
        .agg(
            approx_count_distinct("subject_id").alias("approx_unique_patients"),
            count("value").alias("total_events")
        )
    )

    # Write the real-time aggregations to Snowflake and print progress.
    query_agg = (
        windowed_metrics.writeStream
        .outputMode("update")
        .foreachBatch(process_windowed_metrics)
        .option("checkpointLocation", "tmp/checkpoints/aki_streaming_windowed")
        .start()
    )

    # Apply foreachBatch for custom algorithm demonstrations (Bloom, Reservoir) + metric persistence.
    query_custom = (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_microbatch)
        .option("checkpointLocation", "tmp/checkpoints/aki_streaming_custom")
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
