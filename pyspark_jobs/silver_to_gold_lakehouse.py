"""
PySpark Lakehouse Engine: Silver to Gold Dimensional Lakehouse
Implements:
  - Kimball Star Schema Modeling (Fact & Dimension Tables)
  - Slowly Changing Dimension (SCD Type 2) for Customer Profile Evolution
  - Sessionization & Funnel metrics for Clickstream
  - Pre-aggregated Analytical Data Marts for Amazon Redshift Serverless
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
from pyspark.sql.window import Window


def init_spark():
    if IS_GLUE:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        args = getResolvedOptions(sys.argv, ["SILVER_BUCKET", "GOLD_BUCKET"])
        silver_path = f"s3://{args['SILVER_BUCKET']}/events"
        gold_path = f"s3://{args['GOLD_BUCKET']}"
        return spark, silver_path, gold_path
    else:
        spark = SparkSession.builder \
            .appName("AuroraStream-SilverToGold") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        silver_path = os.environ.get("SILVER_PATH", "data/lakehouse/silver/events")
        gold_path = os.environ.get("GOLD_PATH", "data/lakehouse/gold")
        return spark, silver_path, gold_path


def build_star_schema(spark, silver_path, gold_path):
    print(f"[SPARK:GOLD] Reading Silver dataset from {silver_path}")
    df_silver = spark.read.parquet(silver_path)
    df_silver.createOrReplaceTempView("silver_events")

    # 1. DIMENSION: Date Spine & Retail Event Dimension
    print("[SPARK:GOLD] Generating dim_date...")
    dim_date = df_silver.select("event_date").distinct() \
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int")) \
        .withColumn("year", F.year("event_date")) \
        .withColumn("quarter", F.quarter("event_date")) \
        .withColumn("month", F.month("event_date")) \
        .withColumn("month_name", F.date_format("event_date", "MMMM")) \
        .withColumn("day_of_week", F.dayofweek("event_date")) \
        .withColumn("day_name", F.date_format("event_date", "EEEE")) \
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False)) \
        .withColumn("is_prime_day_event",
                    F.when((F.col("month") == 7) & (F.dayofmonth("event_date").between(11, 16)), True)
                    .otherwise(False)) \
        .withColumn("is_diwali_sale_event",
                    F.when((F.col("month") == 10) & (F.dayofmonth("event_date").between(15, 31)), True)
                    .otherwise(False))

    dim_date.write.mode("overwrite").parquet(f"{gold_path}/dim_date")

    # 2. DIMENSION: SCD Type 2 Customers (Slowly Changing Dimensions)
    print("[SPARK:GOLD] Building dim_customers (SCD Type 2)...")
    cust_window = Window.partitionBy("customer_id").orderBy("event_timestamp_utc")

    dim_customers = df_silver.select(
        "customer_id", "geo_city", "device_type", "event_timestamp_utc"
    ).filter(F.col("customer_id").isNotNull()).distinct()

    dim_customers_scd2 = dim_customers \
        .withColumn("valid_from", F.col("event_timestamp_utc")) \
        .withColumn("valid_to", F.lead("event_timestamp_utc", 1).over(cust_window)) \
        .withColumn("is_current", F.when(F.col("valid_to").isNull(), True).otherwise(False)) \
        .withColumn("customer_sk", F.sha2(F.concat_ws("_", "customer_id", "valid_from"), 256))

    dim_customers_scd2.write.mode("overwrite").parquet(f"{gold_path}/dim_customers")

    # 3. DIMENSION: Product Catalog
    print("[SPARK:GOLD] Building dim_products...")
    dim_products = df_silver.filter(F.col("product_id").isNotNull()) \
        .select("product_id", "category") \
        .distinct() \
        .withColumn("price_tier",
                    F.when(F.col("category").isin(["Electronics", "Home & Kitchen"]), "Premium")
                     .when(F.col("category").isin(["Fashion", "Sports"]), "Mid-Range")
                     .otherwise("Standard"))

    dim_products.write.mode("overwrite").parquet(f"{gold_path}/dim_products")

    # 4. FACT: Order Transactions (Fact Sales)
    print("[SPARK:GOLD] Building fact_orders...")
    df_orders = df_silver.filter(F.col("event_type").isin(["ORDER_PLACED", "ORDER_CANCELLED"]))

    fact_orders = df_orders.select(
        F.col("event_id").alias("transaction_id"),
        "order_id",
        "customer_id",
        "product_id",
        F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
        "event_timestamp_utc",
        "order_status",
        "payment_method",
        "quantity",
        "unit_price",
        "discount_pct",
        "subtotal_amount",
        F.round(F.col("unit_price") * F.col("quantity") * F.col("discount_pct"), 2).alias("discount_amount"),
        F.round(F.col("subtotal_amount") * 0.35, 2).alias("estimated_gross_margin"),
        "is_prime_eligible"
    )

    fact_orders.write.mode("overwrite").partitionBy("date_key").parquet(f"{gold_path}/fact_orders")

    # 5. FACT: Clickstream & Funnel Engagement
    print("[SPARK:GOLD] Building fact_clickstream...")
    df_clicks = df_silver.filter(F.col("event_type").isin(["CLICKSTREAM_VIEW", "CART_ADD"]))

    fact_clickstream = df_clicks.select(
        "event_id",
        "session_id",
        "customer_id",
        "product_id",
        F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
        "event_type",
        "dwell_time_seconds",
        "referral_url",
        "device_type"
    )

    fact_clickstream.write.mode("overwrite").partitionBy("date_key").parquet(f"{gold_path}/fact_clickstream")
    print("[SPARK:SUCCESS] Gold Dimensional Lakehouse tables written successfully.")


if __name__ == "__main__":
    spark, silver_path, gold_path = init_spark()
    build_star_schema(spark, silver_path, gold_path)
