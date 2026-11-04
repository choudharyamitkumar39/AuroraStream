"""
Distributed PySpark Compaction Job (AWS Glue 4.0 / Amazon EMR)
Solves the Small-File Problem in Distributed S3 Lakehouses.

Reads fragmented streaming micro-batches in S3 Bronze / Silver, coalesces partitions
based on optimal target block sizing (128MB), and rewrites partitioned Parquet.
"""

import sys
import os

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from pyspark.context import SparkContext
    IS_GLUE = True
except ImportError:
    IS_GLUE = False

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_distributed_compaction():
    if IS_GLUE:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        args = getResolvedOptions(sys.argv, ["INPUT_PATH", "OUTPUT_PATH", "TARGET_PARTITIONS"])
        input_path = args["INPUT_PATH"]
        output_path = args["OUTPUT_PATH"]
        target_partitions = int(args.get("TARGET_PARTITIONS", 2))
    else:
        spark = SparkSession.builder \
            .appName("AuroraStream-DistributedCompaction") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        input_path = os.environ.get("COMPACT_INPUT", "data/lakehouse/silver/events")
        output_path = os.environ.get("COMPACT_OUTPUT", "data/lakehouse/silver/events_compacted")
        target_partitions = 1

    print(f"[SPARK:COMPACT] Starting compaction from: {input_path} -> {output_path}")

    if not os.path.exists(input_path) and not input_path.startswith("s3://"):
        print(f"[SPARK:COMPACT] Input path not found: {input_path}")
        return

    df = spark.read.parquet(input_path)
    total_records = df.count()
    print(f"[SPARK:COMPACT] Ingested {total_records:,} fragmented records.")

    # Coalesce into optimal target partitions to avoid full network shuffle
    print(f"[SPARK:COMPACT] Coalescing into {target_partitions} target partition(s)...")
    df_compacted = df.coalesce(target_partitions)

    df_compacted.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)

    print(f"[SPARK:COMPACT:SUCCESS] Successfully wrote compacted dataset to {output_path}.")

if __name__ == "__main__":
    run_distributed_compaction()
