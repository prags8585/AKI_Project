#!/usr/bin/env python3
"""Silver layer transforms via PySpark + Snowflake."""

import sys
import os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sf_env import snowflake_spark_options

SF_OPTIONS = snowflake_spark_options()

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
SNOWFLAKE_JAR    = "net.snowflake:spark-snowflake_2.12:3.1.8"
JDBC_JAR         = "net.snowflake:snowflake-jdbc:3.16.1"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("aki-silver-snowflake")
        .config("spark.jars.packages", f"{SNOWFLAKE_JAR},{JDBC_JAR}")
        .getOrCreate()
    )

def read_sf(spark, schema, table):
    return (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**SF_OPTIONS)
        .option("sfSchema", schema)
        .option("dbtable", table)
        .load()
    )

def write_sf(df, schema, table):
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**SF_OPTIONS)
        .option("sfSchema", schema)
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )
    print(f"✅ Written: AKI_DB.{schema}.{table}")

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. ICU Cohort (silver_icu_cohort)
    print("Building silver_icu_cohort...")
    icu = read_sf(spark, "BRONZE", "ICUSTAYS_RAW")
    cohort = (
        icu.select(
            F.col("SUBJECT_ID").cast("long"),
            F.col("HADM_ID").cast("long"),
            F.col("STAY_ID").cast("long"),
            F.col("FIRST_CAREUNIT").cast("string"),
            F.col("LAST_CAREUNIT").cast("string"),
            F.col("INTIME").cast("timestamp"),
            F.col("OUTTIME").cast("timestamp"),
            F.col("LOS").cast("double"),
        )
        .filter(F.col("STAY_ID").isNotNull())
        .filter(F.col("INTIME").isNotNull())
        .filter(F.col("INTIME") < F.col("OUTTIME"))
    )
    write_sf(cohort, "SILVER", "SILVER_ICU_COHORT")

    # 2. Creatinine Events (silver_creatinine_events)
    print("Building silver_creatinine_events...")
    labs = read_sf(spark, "BRONZE", "LABEVENTS_RAW")
    creat = (
        labs.filter(F.col("ITEMID").isin("50912", "51081", "52024", "52546"))
        .select(
            F.col("SUBJECT_ID").cast("long"),
            F.col("HADM_ID").cast("long"),
            F.col("CHARTTIME").cast("timestamp"),
            F.col("VALUENUM").cast("double").alias("creatinine_mg_dl"),
        )
        .filter(F.col("creatinine_mg_dl").isNotNull())
        .filter(F.col("creatinine_mg_dl") > 0)
    )
    # Join with cohort to get stay_id and filter to ICU window
    creat = (
        creat.join(
            cohort.select("SUBJECT_ID", "HADM_ID", "STAY_ID", "INTIME", "OUTTIME"),
            on=["SUBJECT_ID", "HADM_ID"], how="inner"
        )
        .filter((F.col("CHARTTIME") >= F.col("INTIME")) & (F.col("CHARTTIME") <= F.col("OUTTIME")))
        .drop("INTIME", "OUTTIME")
    )
    write_sf(creat, "SILVER", "SILVER_CREATININE_EVENTS")

    # 3. Urine Output Events (silver_urine_output_events)
    print("Building silver_urine_output_events...")
    urine_raw = read_sf(spark, "BRONZE", "OUTPUTEVENTS_RAW")
    urine_itemids = spark.createDataFrame(
        [("226559",), ("226560",), ("226561",), ("226567",), ("226627",), ("226631",), ("227489",)],
        ["ITEMID"],
    )
    urine = (
        urine_raw.join(urine_itemids, on="ITEMID", how="inner")
        .select(
            F.col("SUBJECT_ID").cast("long"),
            F.col("HADM_ID").cast("long"),
            F.col("STAY_ID").cast("long"),
            F.col("CHARTTIME").cast("timestamp"),
            F.col("VALUE").cast("double").alias("urine_ml"),
        )
        .filter(F.col("urine_ml").isNotNull())
        .filter(F.col("urine_ml") >= 0)
    )
    write_sf(urine, "SILVER", "SILVER_URINE_OUTPUT_EVENTS")

    spark.stop()
    print("\n✅ All Silver tables written to Snowflake successfully!")

if __name__ == "__main__":
    main()
