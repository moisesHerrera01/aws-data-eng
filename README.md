# aws-data-eng

Production-oriented proof of concepts for data engineering on AWS, fully implemented as Infrastructure as Code with CloudFormation. Each POC covers a real-world data pipeline pattern using managed AWS services.

---

## POCs

| # | Name | Stack | Status |
|---|------|-------|--------|
| 01 | CDC Postgres → S3 | PostgreSQL, Docker, AWS DMS, S3, SSM, IAM | ✅ Done |
| 02 | Real-time Event Streaming | Kinesis Data Streams, Lambda, DynamoDB, IAM | ✅ Done |
| 03 | Distributed Spark Processing | EMR Serverless, S3, IAM | ✅ Done |
| 04 | Serverless ETL + Data Catalog | Glue, GlueContext, Glue Catalog, Athena, S3 | ✅ Done |
| 05 | GraphQL API on Event Data | AppSync, DynamoDB, IAM | 🔜 Upcoming |

---

## POC 01 — CDC Pipeline: Postgres → S3

Real-time Change Data Capture from a PostgreSQL database into S3 as partitioned Parquet files using AWS DMS. Demonstrates a foundational ingestion pattern for building data lakes from transactional sources.

### Architecture

```
PostgreSQL 15 (Docker)
    │  wal_level=logical
    │  replication slot (pgoutput)
    │
   ngrok (TCP tunnel)
    │
    ▼
AWS DMS
    ├── Replication Instance (t3.medium)
    ├── Source Endpoint  — PostgreSQL via ngrok
    └── Target Endpoint  — S3
         │  format : Parquet
         │  partition: YYYY/MM/DD by change date
         ▼
AWS S3
    public/
    ├── customers/
    │   ├── LOAD00000001.parquet     (initial full load)
    │   └── YYYY/MM/DD/*.parquet     (CDC incremental changes)
    └── orders/
        ├── LOAD00000001.parquet
        └── YYYY/MM/DD/*.parquet
```

### Stack

| Component | Technology | Detail |
|-----------|-----------|--------|
| Source | PostgreSQL 15 | Docker, wal_level=logical |
| Tunnel | ngrok TCP | Exposes port 5432 for DMS connectivity |
| Migration | AWS DMS | full-load-and-cdc migration type |
| Destination | AWS S3 | Parquet, date-partitioned |
| Secrets | AWS SSM Parameter Store | SecureString — never hardcoded |
| IaC | AWS CloudFormation | Full stack in a single template |

### Key Patterns

- **WAL-based CDC**: captures INSERT, UPDATE operations from Postgres logical replication without application changes
- **Full-load + CDC**: DMS performs an initial snapshot then switches to streaming incremental changes
- **Date-partitioned Parquet on S3**: optimized for downstream query engines (Athena, Glue, Spark)
- **SSM Parameter Store**: credentials managed as SecureString, decrypted at deploy time by the shell script — never stored in code or git

### Usage

```bash
# First time only — store credentials in SSM
aws ssm put-parameter --name "poc01-postgres-password" --value "YOUR_PASSWORD" --type SecureString

# 1. Start Postgres
cd poc-01-cdc-postgres-s3/docker
docker-compose up -d

# 2. Expose Postgres via ngrok — update host/port in deploy.sh
ngrok tcp 5432

# 3. Deploy AWS stack
cd ..
bash scripts/deploy.sh

# 4. Tear down when done
bash scripts/destroy.sh
```

### Structure

```
poc-01-cdc-postgres-s3/
├── cloudformation/
│   ├── poc01-cdc.yml           # Full IaC template
│   ├── params.example.json     # Parameter template
│   └── params.json             # Local values (gitignored)
├── docker/
│   ├── docker-compose.yml      # Postgres with logical replication
│   └── init.sql                # Schema and seed data
└── scripts/
    ├── deploy.sh               # Deploys the stack
    └── destroy.sh              # Tears down stack and empties versioned S3 bucket
```

---

## POC 02 — Real-time Event Streaming: Kinesis → DynamoDB

Event-driven pipeline that streams order lifecycle events through Kinesis Data Streams, processes them with a Lambda consumer, and persists state in DynamoDB using a single-table design. Models a real-world e-commerce order tracking system.

### Architecture

```
Producer (Python)
    │  put_record — partition key: order_id
    ▼
Kinesis Data Stream  (1 shard, 24h retention)
    │  trigger — batch size: 10
    ▼
AWS Lambda  (Python 3.12, 128MB)
    │  base64 decode → parse JSON → PutItem
    ▼
DynamoDB  — single-table design
    ├── PK: ORDER#<order_id>
    │   SK: EVENT#<timestamp>#<event_type>
    │   → full event history per order
    │
    └── GSI1PK: CUSTOMER#<customer_id>
        GSI1SK: <timestamp>
        → all orders per customer
```

### Stack

| Component | Technology | Detail |
|-----------|-----------|--------|
| Producer | Python + boto3 | Simulates app generating order events |
| Stream | Kinesis Data Streams | 1 shard, 24h retention |
| Consumer | AWS Lambda | Kinesis trigger, batch=10, Python 3.12 |
| Storage | AWS DynamoDB | On-demand billing, single-table design |
| Observability | CloudWatch Logs | 3-day retention |
| IaC | AWS CloudFormation | Full stack in a single template |

### Key Patterns

- **Event sourcing**: each state transition (placed → confirmed → shipped → delivered) is an immutable event — state is derived from history
- **Single-table design**: PK + SK model enables multiple access patterns (by order, by customer) in one DynamoDB table with no joins
- **Partition key strategy**: `order_id` as Kinesis partition key ensures all events for the same order land on the same shard — preserving event ordering
- **GSI for secondary access**: queries by customer without full table scans

### Usage

```bash
# Deploy AWS stack
cd poc-02-kinesis-dynamo
bash scripts/deploy.sh

# Send events — simulate order lifecycle
python scripts/producer.py --orders 5

# Continuous mode
python scripts/producer.py --continuous

# Tear down when done
bash scripts/destroy.sh
```

### Structure

```
poc-02-kinesis-dynamo/
├── cloudformation/
│   └── poc02-kinesis-dynamo.yml  # Full IaC template
├── lambda/
│   └── processor.py              # Kinesis consumer — readable reference
└── scripts/
    ├── producer.py               # Order event simulator
    ├── deploy.sh                 # Deploys the stack
    └── destroy.sh                # Tears down the stack
```

---

## POC 03 — Distributed Spark Processing: Medallion Architecture on EMR Serverless

Batch data processing pipeline implementing the Medallion Architecture (Bronze → Silver → Gold) using EMR Serverless with PySpark. Demonstrates distributed transformations, schema enforcement, and multi-layer aggregation on S3 as a data lake.

### Architecture

```
S3 Bronze  (raw CSV — simulates DMS CDC output)
    ├── bronze/orders/orders.csv
    └── bronze/customers/customers.csv
         │
         ▼
EMR Serverless — Job 1: bronze_to_silver.py
    - filter CDC deletes (op != 'D')
    - deduplicate by primary key
    - cast and enforce schema
    - partition by year/month
         │
         ▼
S3 Silver  (clean Parquet, partitioned)
    ├── silver/orders/year=2026/month=4/
    └── silver/customers/
         │
         ▼
EMR Serverless — Job 2: silver_to_gold.py
    - daily_sales       : revenue KPIs per day
    - product_ranking   : units sold + revenue with dense_rank()
    - customer_summary  : join orders + customers + top product via Window
         │
         ▼
S3 Gold  (aggregated Parquet — ready for Athena / Glue Catalog)
    ├── gold/daily_sales/
    ├── gold/product_ranking/
    └── gold/customer_summary/
```

### Stack

| Component | Technology | Detail |
|-----------|-----------|--------|
| Compute | EMR Serverless (emr-7.0.0 / Spark 3.5) | Auto-stop after 1 min idle, pay per vCPU-second |
| Storage | AWS S3 | Bronze / Silver / Gold layers |
| IAM | Execution Role | Least-privilege S3 + CloudWatch access |
| IaC | AWS CloudFormation | Full stack in a single template |

### Key Patterns

- **Medallion Architecture**: Bronze (raw) → Silver (clean, typed) → Gold (aggregated) — each layer has a clear contract
- **Partition pruning**: Silver partitioned by `year/month` — downstream queries scan only relevant partitions
- **Window functions**: `dense_rank()` for product revenue ranking, `rank()` for top product per customer
- **Schema enforcement in Silver**: explicit casting + `trim/upper` normalization — Gold never sees dirty data
- **EMR Serverless vs Glue**: pure Spark, no vendor lock-in, ~4x cheaper than Glue DPU pricing

### Usage

```bash
# Deploy stack (S3 + EMR Serverless Application + IAM)
cd poc-03-emr-spark
bash scripts/deploy.sh

# Run full pipeline: generate data -> Bronze->Silver -> Silver->Gold
bash scripts/submit_jobs.sh

# Tear down when done (empties S3 first)
bash scripts/destroy.sh
```

### Structure

```
poc-03-emr-spark/
├── cloudformation/
│   └── poc03-emr-spark.yml     # S3, EMR Serverless app, IAM role
├── spark/
│   ├── bronze_to_silver.py     # PySpark Job 1 — clean and partition
│   └── silver_to_gold.py       # PySpark Job 2 — aggregate and rank
└── scripts/
    ├── generate_data.py        # Generates Bronze CSV seed data in S3
    ├── deploy.sh               # Deploys the CloudFormation stack
    ├── submit_jobs.sh          # Uploads scripts + submits EMR jobs in sequence
    └── destroy.sh              # Empties S3 and tears down the stack
```

---

## POC 04 — Serverless ETL + Data Catalog: Glue + Athena

Incremental ETL pipeline using AWS Glue with Job Bookmarks to process only new files on each run, auto-cataloging schema into the Glue Data Catalog and querying results with Athena. Demonstrates the foundational serverless analytics pattern for data lakes on S3.

### Architecture

```
S3 Bronze  (raw CSV — simulated CDC output)
    ├── bronze/orders/batch1_orders.csv
    └── bronze/orders/batch2_orders.csv
         │
    Glue Crawler (bronze)
    → auto-discovers schema → Glue Catalog: bronze.orders
         │
         ▼
AWS Glue ETL Job  (poc04-bronze-to-silver)
    - Job Bookmark: tracks processed files — skips batch1 on second run
    - DynamicFrame: read CSV, resolve types, rename fields
    - filter op != 'D', trim/upper normalization
    - write Parquet partitioned by year/month to Silver
         │
    Glue Crawler (silver)
    → auto-discovers schema → Glue Catalog: silver.orders
         │
         ▼
S3 Silver  (clean Parquet, partitioned)
    └── silver/orders/year=2026/month=4/
         │
         ▼
Amazon Athena  (poc04-workgroup)
    SELECT status, COUNT(*), SUM(amount)
    FROM silver.orders GROUP BY status
```

### Stack

| Component | Technology | Detail |
|-----------|-----------|--------|
| Storage | AWS S3 | Bronze (CSV) and Silver (Parquet) layers |
| ETL | AWS Glue ETL (v4.0) | GlueContext, DynamicFrame, Job Bookmark |
| Catalog | AWS Glue Data Catalog | Databases + Crawlers per layer (bronze/silver/gold) |
| Query | Amazon Athena | Serverless SQL over S3, custom Workgroup |
| IAM | Glue Execution Role | AWSGlueServiceRole + least-privilege S3 inline policy |
| IaC | AWS CloudFormation | Full stack in a single template |

### Key Patterns

- **Job Bookmark**: Glue tracks which S3 files have already been processed — batch2 run reads only the 30 new records, not the original 50
- **GlueContext vs SparkContext**: `DynamicFrame` handles schema ambiguity natively; `resolveChoice` and `apply_mapping` replace manual casts
- **Crawlers per layer**: Bronze crawler discovers raw CSV schema; Silver crawler picks up partitioned Parquet — both write to the Glue Catalog automatically
- **Athena Workgroup**: query results isolated per project, `RecursiveDeleteOption` ensures clean teardown even with query history
- **Glue vs EMR Serverless**: Glue adds Job Bookmarks, Catalog integration, and managed connectors; EMR Serverless gives raw Spark with lower per-DPU cost and no vendor lock-in

### Usage

```bash
# Deploy stack (S3 + Glue + Crawlers + Athena Workgroup)
cd poc-04-glue-catalog
bash scripts/deploy.sh

# Run initial load (batch1 — 50 orders)
bash scripts/run_pipeline.sh

# Run incremental load (batch2 — 30 new orders, Job Bookmark skips batch1)
bash scripts/run_pipeline.sh batch2

# Tear down when done
bash scripts/destroy.sh
```

### Structure

```
poc-04-glue-catalog/
├── cloudformation/
│   └── poc04-glue-catalog.yml   # S3, Glue databases, crawlers, ETL job, Athena workgroup
├── glue/
│   └── etl_job.py               # GlueContext ETL — Bronze CSV to Silver Parquet with bookmark
└── scripts/
    ├── generate_data.py          # Generates Bronze CSV batches (batch1 / batch2)
    ├── deploy.sh                 # Deploys the CloudFormation stack
    ├── run_pipeline.sh           # End-to-end: generate -> crawl -> ETL -> query
    └── destroy.sh                # Empties S3 buckets and tears down the stack
```

---
