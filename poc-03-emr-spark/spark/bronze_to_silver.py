"""
Job 1: Bronze -> Silver
- Reads raw CSV from S3 Bronze layer (simulates DMS CDC output)
- Cleans, deduplicates, enforces schema
- Writes Parquet partitioned by year/month to Silver layer
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, year, month,
    current_timestamp, when, trim, upper
)
from pyspark.sql.types import DoubleType, IntegerType


def build_spark():
    return SparkSession.builder \
        .appName("bronze-to-silver") \
        .getOrCreate()


def process_orders(spark, bucket):
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv(f"s3://{bucket}/bronze/orders/")

    silver = df \
        .filter(col("op") != "D") \
        .dropDuplicates(["order_id"]) \
        .withColumn("customer_id",  col("customer_id").cast(IntegerType())) \
        .withColumn("quantity",     col("quantity").cast(IntegerType())) \
        .withColumn("amount",       col("amount").cast(DoubleType())) \
        .withColumn("status",       trim(upper(col("status")))) \
        .withColumn("created_at",   to_timestamp("created_at")) \
        .withColumn("updated_at",   to_timestamp("updated_at")) \
        .withColumn("year",         year("created_at")) \
        .withColumn("month",        month("created_at")) \
        .withColumn("processed_at", current_timestamp()) \
        .drop("op")

    silver.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"s3://{bucket}/silver/orders/")

    count = silver.count()
    print(f"[orders] Silver records written: {count}")
    return count


def process_customers(spark, bucket):
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", False) \
        .csv(f"s3://{bucket}/bronze/customers/")

    silver = df \
        .filter(col("op") != "D") \
        .dropDuplicates(["customer_id"]) \
        .withColumn("customer_id",  col("customer_id").cast(IntegerType())) \
        .withColumn("country",      trim(upper(col("country")))) \
        .withColumn("created_at",   to_timestamp("created_at")) \
        .withColumn("processed_at", current_timestamp())  \
        .drop("op")

    silver.write \
        .mode("overwrite") \
        .parquet(f"s3://{bucket}/silver/customers/")

    count = silver.count()
    print(f"[customers] Silver records written: {count}")
    return count


def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: bronze_to_silver.py <bucket_name>")

    bucket = sys.argv[1]
    spark  = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Starting Bronze -> Silver | bucket: {bucket}")
    o = process_orders(spark, bucket)
    c = process_customers(spark, bucket)
    print(f"Bronze -> Silver complete | orders={o} customers={c}")

    spark.stop()


if __name__ == "__main__":
    main()
