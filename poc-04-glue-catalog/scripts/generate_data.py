"""
Generates Bronze CSV batches and uploads to S3.
Supports two modes:
  batch1 — initial load (50 orders, 5 customers)
  batch2 — incremental load (30 new orders, 2 new customers)

Usage:
  python generate_data.py <bucket> batch1
  python generate_data.py <bucket> batch2
"""
import boto3
import csv
import io
import sys
import uuid
import random
from datetime import datetime, timedelta, timezone

REGION = "us-east-1"

PRODUCTS = [
    ("Laptop",          1200.00),
    ("Monitor",          350.00),
    ("Keyboard",          89.99),
    ("Headset",          149.99),
    ("Webcam",            79.99),
    ("Mouse",             45.00),
    ("Tablet",           499.00),
    ("Docking Station",  199.99),
]

STATUSES  = ["completed", "shipped", "pending", "cancelled"]

CUSTOMERS_BATCH1 = [
    (1, "Alice Smith",  "alice@example.com",  "US"),
    (2, "Bob Johnson",  "bob@example.com",    "CA"),
    (3, "Carlos Lopez", "carlos@example.com", "MX"),
    (4, "Diana Prince", "diana@example.com",  "UK"),
    (5, "Eve Torres",   "eve@example.com",    "BR"),
]

CUSTOMERS_BATCH2 = [
    (6, "Frank Miller", "frank@example.com",  "DE"),
    (7, "Grace Lee",    "grace@example.com",  "KR"),
]


def random_ts(days_back_min=0, days_back_max=30):
    base   = datetime.now(timezone.utc) - timedelta(days=days_back_max)
    offset = timedelta(
        days=random.randint(0, days_back_max - days_back_min),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return (base + offset).strftime("%Y-%m-%d %H:%M:%S")


def make_orders(n, customer_ids, days_back_min=0, days_back_max=30):
    rows = []
    for _ in range(n):
        product, price = random.choice(PRODUCTS)
        qty = random.randint(1, 5)
        ts  = random_ts(days_back_min, days_back_max)
        rows.append({
            "order_id":    str(uuid.uuid4()),
            "customer_id": random.choice(customer_ids),
            "product":     product,
            "quantity":    qty,
            "amount":      round(price * qty, 2),
            "status":      random.choices(STATUSES, weights=[40, 30, 20, 10])[0],
            "created_at":  ts,
            "updated_at":  ts,
            "op":          random.choices(["I", "U"], weights=[80, 20])[0],
        })
    return rows


def make_customers(customers):
    return [{
        "customer_id": cid,
        "name":        name,
        "email":       email,
        "country":     country,
        "created_at":  random_ts(0, 60),
        "op":          "I",
    } for cid, name, email, country in customers]


def to_csv_bytes(rows, fieldnames):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def upload(s3, bucket, key, data):
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"  uploaded: s3://{bucket}/{key}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python generate_data.py <bucket> <batch1|batch2>")
        sys.exit(1)

    bucket = sys.argv[1]
    batch  = sys.argv[2]
    s3     = boto3.client("s3", region_name=REGION)

    ORDER_FIELDS    = ["order_id","customer_id","product","quantity","amount","status","created_at","updated_at","op"]
    CUSTOMER_FIELDS = ["customer_id","name","email","country","created_at","op"]

    if batch == "batch1":
        print(">>> Uploading BATCH 1 (initial load)...")
        orders    = make_orders(50, [1,2,3,4,5], days_back_max=30)
        customers = make_customers(CUSTOMERS_BATCH1)
        upload(s3, bucket, "bronze/orders/batch1_orders.csv",       to_csv_bytes(orders, ORDER_FIELDS))
        upload(s3, bucket, "bronze/customers/batch1_customers.csv", to_csv_bytes(customers, CUSTOMER_FIELDS))
        print(f"Batch 1 ready: {len(orders)} orders, {len(customers)} customers")

    elif batch == "batch2":
        print(">>> Uploading BATCH 2 (incremental - new records only)...")
        orders    = make_orders(30, [1,2,3,4,5,6,7], days_back_max=3)
        customers = make_customers(CUSTOMERS_BATCH2)
        upload(s3, bucket, "bronze/orders/batch2_orders.csv",       to_csv_bytes(orders, ORDER_FIELDS))
        upload(s3, bucket, "bronze/customers/batch2_customers.csv", to_csv_bytes(customers, CUSTOMER_FIELDS))
        print(f"Batch 2 ready: {len(orders)} orders, {len(customers)} customers")
        print("Job Bookmark will only process these NEW files on the next job run.")

    else:
        print(f"Unknown batch: {batch}. Use batch1 or batch2.")
        sys.exit(1)


if __name__ == "__main__":
    main()
