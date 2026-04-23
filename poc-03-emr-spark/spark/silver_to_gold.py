"""
Job 2: Silver -> Gold
- Reads clean Parquet from Silver layer
- Produces 3 aggregated Gold tables:
    1. daily_sales       — revenue KPIs per day
    2. product_ranking   — product performance with window ranking
    3. customer_summary  — per-customer order metrics
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, countDistinct,
    to_date, round, rank, dense_rank
)
from pyspark.sql.window import Window


def build_spark():
    return SparkSession.builder \
        .appName("silver-to-gold") \
        .getOrCreate()


def daily_sales(orders, bucket):
    df = orders \
        .withColumn("date", to_date("created_at")) \
        .groupBy("date") \
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("amount"), 2).alias("total_revenue"),
            round(avg("amount"), 2).alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("date")

    df.write.mode("overwrite").parquet(f"s3://{bucket}/gold/daily_sales/")
    print(f"[daily_sales] Gold records: {df.count()}")


def product_ranking(orders, bucket):
    window = Window.orderBy(col("total_revenue").desc())

    df = orders \
        .groupBy("product") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_units_sold"),
            round(sum("amount"), 2).alias("total_revenue"),
            round(avg("amount"), 2).alias("avg_unit_price")
        ) \
        .withColumn("revenue_rank", dense_rank().over(window))

    df.write.mode("overwrite").parquet(f"s3://{bucket}/gold/product_ranking/")
    print(f"[product_ranking] Gold records: {df.count()}")


def customer_summary(orders, customers, bucket):
    # Most purchased product per customer using window function
    product_window = Window \
        .partitionBy("customer_id") \
        .orderBy(col("product_orders").desc())

    top_product = orders \
        .groupBy("customer_id", "product") \
        .agg(count("order_id").alias("product_orders")) \
        .withColumn("rank", rank().over(product_window)) \
        .filter(col("rank") == 1) \
        .select("customer_id", col("product").alias("top_product"))

    df = orders \
        .groupBy("customer_id") \
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("amount"), 2).alias("total_spent"),
            round(avg("amount"), 2).alias("avg_order_value"),
            max("created_at").alias("last_order_date")
        ) \
        .join(customers.select("customer_id", "name", "email", "country"),
              on="customer_id", how="left") \
        .join(top_product, on="customer_id", how="left")

    df.write.mode("overwrite").parquet(f"s3://{bucket}/gold/customer_summary/")
    print(f"[customer_summary] Gold records: {df.count()}")


def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: silver_to_gold.py <bucket_name>")

    bucket    = sys.argv[1]
    spark     = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Starting Silver -> Gold | bucket: {bucket}")

    orders    = spark.read.parquet(f"s3://{bucket}/silver/orders/")
    customers = spark.read.parquet(f"s3://{bucket}/silver/customers/")

    daily_sales(orders, bucket)
    product_ranking(orders, bucket)
    customer_summary(orders, customers, bucket)

    print("Silver -> Gold complete")
    spark.stop()


if __name__ == "__main__":
    main()
