#!/usr/bin/env python3
"""
Kafka Producer for AKI Pipeline — Snowflake backend.
Pulls Silver layer distributions from Snowflake to calibrate synthetic data,
then generates an infinite stream of realistic synthetic events into Kafka.
"""

import json
import time
import random
import os
from datetime import datetime, timedelta
import snowflake.connector

try:
    from confluent_kafka import Producer
except ImportError:
    print("confluent_kafka not found. Run: pip install confluent_kafka")
    Producer = None

from scripts.sf_env import snowflake_connect_kwargs

SF_CONN = snowflake_connect_kwargs()

def get_distributions():
    """Pull real clinical distributions from Snowflake Silver layer."""
    print("Fetching baseline distributions from Snowflake Silver layer...")
    conn = snowflake.connector.connect(**SF_CONN)
    cur = conn.cursor()

    cur.execute("SELECT AVG(CREATININE_MG_DL), STDDEV(CREATININE_MG_DL) FROM AKI_DB.SILVER.SILVER_CREATININE_EVENTS")
    creat_mean, creat_std = cur.fetchone()

    cur.execute("SELECT AVG(URINE_ML), STDDEV(URINE_ML) FROM AKI_DB.SILVER.SILVER_URINE_OUTPUT_EVENTS")
    urine_mean, urine_std = cur.fetchone()

    cur.close()
    conn.close()

    return {
        "creat_mean": creat_mean or 1.0,
        "creat_std":  creat_std  or 0.3,
        "urine_mean": urine_mean or 50.0,
        "urine_std":  urine_std  or 20.0,
    }

def generate_synthetic_event(patient_id, distributions, current_time):
    is_creat = random.random() > 0.5
    is_sick  = (patient_id % 5 == 0)

    if is_creat:
        val = random.gauss(distributions["creat_mean"], distributions["creat_std"])
        if is_sick:
            val += random.uniform(1.0, 3.5)
        return {
            "subject_id": patient_id,
            "hadm_id":    patient_id * 10,
            "stay_id":    patient_id * 100,
            "charttime":  current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": "creatinine",
            "value":      max(0.2, round(val, 2)),
            "uom":        "mg/dL",
        }
    else:
        val = random.gauss(distributions["urine_mean"], distributions["urine_std"])
        if is_sick:
            val -= random.uniform(10.0, 30.0)
        return {
            "subject_id": patient_id,
            "hadm_id":    patient_id * 10,
            "stay_id":    patient_id * 100,
            "charttime":  current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": "urine",
            "value":      max(0.0, round(val, 2)),
            "uom":        "mL",
        }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker",          default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic",           default=os.getenv("KAFKA_TOPIC", "aki.live.events"))
    parser.add_argument("--events_per_sec",  type=int, default=5)
    args = parser.parse_args()

    distributions = get_distributions()
    print(f"Distributions loaded: {distributions}")

    if Producer is None:
        producer = None
        print("DRY-RUN mode (no Kafka)")
    else:
        producer = Producer({"bootstrap.servers": args.broker})
        print(f"Connected to Kafka at {args.broker}")

    print("Starting infinite synthetic stream... (Ctrl+C to stop)")
    patient_pool   = list(range(9000000, 9000100))
    current_time   = datetime.now()

    try:
        while True:
            patient_id = random.choice(patient_pool)
            event      = generate_synthetic_event(patient_id, distributions, current_time)
            payload    = json.dumps(event)

            if producer:
                producer.produce(args.topic, payload.encode("utf-8"), callback=delivery_report)
                producer.poll(0)

            print(f"PRODUCED -> {payload}")
            current_time += timedelta(minutes=random.randint(1, 15))
            time.sleep(1.0 / args.events_per_sec)
    except KeyboardInterrupt:
        print("\nStopping synthetic stream.")
    finally:
        if producer:
            producer.flush()

if __name__ == "__main__":
    main()
