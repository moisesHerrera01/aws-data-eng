"""
Glue ETL Job — Bronze -> Silver (incremental with Job Bookmark)

Job Bookmark tracks the last processed S3 file position.
On each run, Glue only reads files added since the previous run —
no CDC required on the source, no manual watermark management.

Args (Glue job parameters):
  --BUCKET : S3 data lake bucket name
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, to_timestamp, year, month,
    current_timestamp, trim, upper
)
from pyspark.sql.types import DoubleType, IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Job Bookmark init — Glue tracks position from here
job.init(args["JOB_NAME"], args)


def process_orders():
    # GlueContext reads only NEW files since last bookmark checkpoint
    raw = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"s3://{bucket}/bronze/orders/"],
            "recurse": True,
        },
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx="orders_source",  # bookmark key per datasource
    )

    if raw.count() == 0:
        print("[orders] No new records since last run — bookmark active")
        return 0

    df = raw.toDF()

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

    out = DynamicFrame.fromDF(silver, glueContext, "orders_silver")

    glueContext.write_dynamic_frame.from_options(
        frame=out,
        connection_type="s3",
        connection_options={"path": f"s3://{bucket}/silver/orders/"},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="orders_sink",
    )

    count = silver.count()
    print(f"[orders] Silver records written: {count}")
    return count


def process_customers():
    raw = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"s3://{bucket}/bronze/customers/"],
            "recurse": True,
        },
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx="customers_source",
    )

    if raw.count() == 0:
        print("[customers] No new records since last run — bookmark active")
        return 0

    df = raw.toDF()

    silver = df \
        .filter(col("op") != "D") \
        .dropDuplicates(["customer_id"]) \
        .withColumn("customer_id",  col("customer_id").cast(IntegerType())) \
        .withColumn("country",      trim(upper(col("country")))) \
        .withColumn("created_at",   to_timestamp("created_at")) \
        .withColumn("processed_at", current_timestamp()) \
        .drop("op")

    out = DynamicFrame.fromDF(silver, glueContext, "customers_silver")

    glueContext.write_dynamic_frame.from_options(
        frame=out,
        connection_type="s3",
        connection_options={"path": f"s3://{bucket}/silver/customers/"},
        format="parquet",
        format_options={"compression": "snappy"},
        transformation_ctx="customers_sink",
    )

    count = silver.count()
    print(f"[customers] Silver records written: {count}")
    return count


o = process_orders()
c = process_customers()
print(f"Bronze -> Silver complete | orders={o} customers={c}")

# Commit bookmark — marks current position as processed
job.commit()
