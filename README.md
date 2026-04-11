# aws-data-eng

POCs de servicios AWS para data engineering, implementados como IaC con CloudFormation.

---

## POCs

| # | Nombre | Stack | Estado |
|---|--------|-------|--------|
| 01 | CDC Postgres → S3 | Docker, Postgres, ngrok, DMS, S3, SSM, IAM | ✅ Completo |
| 02 | Kinesis → DynamoDB | Kinesis Data Streams, DynamoDB, Lambda | 🔜 Pendiente |
| 03 | EMR + Spark | EMR, S3, IAM | 🔜 Pendiente |
| 04 | Glue + Data Catalog | Glue, PySpark, Glue Catalog, Athena, S3 | 🔜 Pendiente |
| 05 | AppSync API | AppSync, DynamoDB, IAM | 🔜 Pendiente |

---

## POC 01 — CDC Postgres a S3 via DMS

Captura de cambios en tiempo real (CDC) desde una base de datos PostgreSQL local hacia S3 en formato Parquet, usando AWS DMS.

### Arquitectura

```
Postgres 15 (Docker)
    │  wal_level=logical
    │  replication slot (test_decoding)
    │
   ngrok (TCP tunnel)
    │
    ▼
AWS DMS
    ├── Replication Instance (t3.medium)
    ├── Source Endpoint (PostgreSQL via ngrok)
    └── Target Endpoint (S3)
         │  formato: Parquet
         │  partición: YYYY/MM/DD por fecha de cambio
         ▼
AWS S3
    public/
    ├── customers/
    │   ├── LOAD00000001.parquet        (full load inicial)
    │   └── YYYY/MM/DD/*.parquet        (cambios CDC)
    └── orders/
        ├── LOAD00000001.parquet
        └── YYYY/MM/DD/*.parquet
```

### Stack

| Componente | Tecnología | Detalle |
|------------|-----------|---------|
| Fuente | PostgreSQL 15 | Docker, wal_level=logical |
| Tunnel | ngrok TCP | Expone puerto 5432 a internet |
| Migración | AWS DMS | full-load-and-cdc, MigrationType |
| Destino | AWS S3 | Parquet, partición por fecha |
| Secretos | AWS SSM Parameter Store | SecureString para password |
| IaC | AWS CloudFormation | Stack completo en un template |

### Conceptos clave aprendidos

- **WAL (Write-Ahead Log)**: log binario de Postgres donde DMS lee los cambios
- **Replication Slot**: cursor que DMS mantiene para no perder eventos del WAL
- **test_decoding / pgoutput**: plugins que decodifican el WAL binario a formato legible
- **full-load-and-cdc**: DMS primero carga todo y luego captura cambios incrementales
- **CdcInsertsAndUpdates**: configuración que excluye DELETEs del CDC en S3

### Uso

```bash
# Prerequisito — crear secreto en SSM (solo la primera vez)
aws ssm put-parameter --name "poc01-postgres-password" --value "TU_PASSWORD" --type SecureString

# 1. Levantar Postgres
cd poc-01-cdc-postgres-s3/docker
docker-compose up -d

# 2. Exponer Postgres via ngrok y actualizar host/puerto en deploy.sh
ngrok tcp 5432

# 3. Desplegar stack AWS
cd ..
bash scripts/deploy.sh

# 4. Al terminar — destruir recursos para evitar costos
bash scripts/destroy.sh
```

### Estructura

```
poc-01-cdc-postgres-s3/
├── cloudformation/
│   ├── poc01-cdc.yml           # Template IaC completo
│   ├── params.example.json     # Plantilla de parámetros (copiar a params.json)
│   └── params.json             # Valores locales (ignorado por git)
├── docker/
│   ├── docker-compose.yml      # Postgres con logical replication
│   └── init.sql                # Schema y datos semilla
└── scripts/
    ├── deploy.sh               # Despliega el stack
    └── destroy.sh              # Destruye el stack y vacía el bucket
```

---
