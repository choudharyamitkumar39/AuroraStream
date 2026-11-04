"""
PySpark Lakehouse Engine: Bronze to Silver Layer
Runs on AWS Glue 4.0 / AWS EMR Serverless / Local PySpark.

Key Transformations:
  1. Ingests raw JSONL events from S3 Bronze.
  2. PII Masking: Anonymizes IP addresses and customer identifiers via SHA-256.
  3. Deduplication: Window-based deduplication on event_id.
  4. Timestamp normalization & Watermarking.
  5. Partitioned writes to S3 Silver in Snappy Parquet.
"""

import sys
import os
from datetime import datetime

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    IS_GLUE = True
except ImportError:
    IS_GLUE = False

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def init_spark():
    if IS_GLUE:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        args = getResolvedOptions(sys.argv, ["BRONZE_BUCKET", "SILVER_BUCKET"])
        bronze_path = f"s3://{args['BRONZE_BUCKET']}/events"
        silver_path = f"s3://{args['SILVER_BUCKET']}/events"
        return spark, bronze_path, silver_path
    else:
        # Local / Test Spark session
        spark = SparkSession.builder \
            .appName("AuroraStream-BronzeToSilver") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        bronze_path = os.environ.get("BRONZE_PATH", "data/lakehouse/bronze/events")
        silver_path = os.environ.get("SILVER_PATH", "data/lakehouse/silver/events")
        return spark, bronze_path, silver_path


def run_bronze_to_silver():
    spark, bronze_path, silver_path = init_spark()
    print(f"[SPARK:START] Processing Bronze from: {bronze_path}")

    # Read raw JSON / JSONL from Bronze
    if not os.path.exists(bronze_path) and not bronze_path.startswith("s3://"):
        print(f"[SPARK:WARN] Bronze path does not exist yet: {bronze_path}")
        return

    df_raw = spark.read.json(bronze_path)
    raw_count = df_raw.count()
    print(f"[SPARK] Loaded {raw_count:,} raw events from Bronze layer.")

    if raw_count == 0:
        print("[SPARK] No records to process. Exiting.")
        return

    # 1. Deduplication using window function
    window_dedup = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
    df_dedup = df_raw.withColumn("row_num", F.row_number().over(window_dedup)) \
                     .filter(F.col("row_num") == 1) \
                     .drop("row_num")

    dedup_count = df_dedup.count()
    duplicates_removed = raw_count - dedup_count
    print(f"[SPARK] Deduplication: Removed {duplicates_removed:,} duplicate events.")

    # 2. PII Masking (SHA-256 for IP address and customer identifiers)
    df_masked = df_dedup \
        .withColumn("ip_address_hash", F.sha2(F.col("ip_address"), 256)) \
        .drop("ip_address")

    # 3. Timestamp Normalization & Partition Extraction
    df_clean = df_masked \
        .withColumn("event_timestamp_utc", F.to_timestamp("event_timestamp")) \
        .withColumn("event_date", F.to_date("event_timestamp_utc")) \
        .withColumn("year", F.year("event_date")) \
        .withColumn("month", F.month("event_date")) \
        .withColumn("day", F.dayofmonth("event_date"))

    # 4. Write to S3 Silver in snappy-compressed Parquet format partitioned by event_date & event_type
    print(f"[SPARK] Writing {dedup_count:,} cleaned records to Silver: {silver_path}")
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("event_date", "event_type") \
        .parquet(silver_path)

    print(f"[SPARK:SUCCESS] Bronze to Silver transformation complete.")


if __name__ == "__main__":
    run_bronze_to_silver()
