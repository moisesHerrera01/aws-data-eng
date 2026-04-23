"""
Generates Bronze layer seed data and uploads to S3.
Simulates CDC output from DMS (same format as POC 01).

Usage:
  python generate_data.py <bucket_name>
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
    ("Laptop",         1200.00),
    ("Monitor",         350.00),
    ("Keyboard",         89.99),
    ("Headset",         149.99),
    ("Webcam",           79.99),
    ("Mouse",            45.00),
    ("Tablet",          499.00),
    ("Docking Station", 199.99),
]

STATUSES   = ["completed", "shipped", "pending", "cancelled"]
CUSTOMERS  = [
    (1, "Alice Smith",   "alice@example.com",  "US"),
    (2, "Bob Johnson",   "bob@example.com",    "CA"),
    (3, "Carlos Lopez",  "carlos@example.com", "MX"),
    (4, "Diana Prince",  "diana@example.com",  "UK"),
    (5, "Eve Torres",    "eve@example.com",    "BR"),
]


def random_ts(days_back=30):
    base = datetime.now(timezone.utc) - timedelta(days=days_back)
    offset = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    return (base + offset).strftime("%Y-%m-%d %H:%M:%S")


def generate_orders(n=100):
    rows = []
    for _ in range(n):
        product, price = random.choice(PRODUCTS)
        qty = random.randint(1, 5)
        created = random_ts(30)
        rows.append({
            "order_id":    str(uuid.uuid4()),
            "customer_id": random.choice(CUSTOMERS)[0],
            "product":     product,
            "quantity":    qty,
            "amount":      round(price * qty, 2),
            "status":      random.choices(STATUSES, weights=[40, 30, 20, 10])[0],
            "created_at":  created,
            "updated_at":  created,
            "op":          random.choices(["I", "U"], weights=[70, 30])[0],
        })
    return rows


def generate_customers():
    rows = []
    for cid, name, email, country in CUSTOMERS:
        rows.append({
            "customer_id": cid,
            "name":        name,
            "email":       email,
            "country":     country,
            "created_at":  random_ts(60),
            "op":          "I",
        })
    return rows


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
    if len(sys.argv) < 2:
        print("Usage: python generate_data.py <bucket_name>")
        sys.exit(1)

    bucket = sys.argv[1]
    s3     = boto3.client("s3", region_name=REGION)

    print(f"Generating Bronze data -> s3://{bucket}/bronze/")

    orders    = generate_orders(100)
    customers = generate_customers()

    upload(s3, bucket, "bronze/orders/orders.csv",
           to_csv_bytes(orders,
               ["order_id","customer_id","product","quantity","amount","status","created_at","updated_at","op"]))

    upload(s3, bucket, "bronze/customers/customers.csv",
           to_csv_bytes(customers,
               ["customer_id","name","email","country","created_at","op"]))

    print(f"\nBronze layer ready: {len(orders)} orders, {len(customers)} customers")


if __name__ == "__main__":
    main()
