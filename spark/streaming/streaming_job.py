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
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, approx_count_distinct, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

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

def process_microbatch(batch_df, batch_id):
    """
    Applied to every micro-batch of data arriving from Kafka.
    """
    print(f"\n--- Processing Micro-Batch {batch_id} ---")
    
    # 1. Bloom Filter Deduplication
    # We collect the batch and filter duplicates using our Bloom Filter
    events = batch_df.collect()
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


def main():
    spark = (
        SparkSession.builder.appName("aki-streaming-consumer")
        # Include Kafka SQL package
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

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

    print("Connecting to Kafka...")
    
    # Read from Kafka topic
    # Using rate stream for testing if Kafka is unavailable, but normally use "kafka" format
    try:
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "aki.live.events")
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

    # Write the real-time aggregations to the console
    query_agg = (
        windowed_metrics.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    # Apply foreachBatch for custom algorithm demonstrations (Bloom, Reservoir)
    query_custom = (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_microbatch)
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
