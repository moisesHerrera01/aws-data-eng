# aws-data-eng

Production-oriented proof of concepts for data engineering on AWS, fully implemented as Infrastructure as Code with CloudFormation. Each POC covers a real-world data pipeline pattern using managed AWS services.

---

## POCs

| # | Name | Stack | Status |
|---|------|-------|--------|
| 01 | CDC Postgres → S3 | PostgreSQL, Docker, AWS DMS, S3, SSM, IAM | ✅ Done |
| 02 | Real-time Event Streaming | Kinesis Data Streams, Lambda, DynamoDB, IAM | ✅ Done |
| 03 | Distributed Spark Processing | EMR, S3, IAM | 🔜 Upcoming |
| 04 | Serverless ETL + Data Catalog | Glue, PySpark, Glue Catalog, Athena, S3 | 🔜 Upcoming |
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
