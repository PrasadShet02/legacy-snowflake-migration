# Enterprise Trade Data Migration Pipeline

This repository contains a robust, event-driven data pipeline engineered to ingest unpredictable, massive-scale trade data payloads from legacy on-premises systems into a cloud-native Snowflake data warehouse. 

The architecture guarantees zero-duplicate loads, minimizes cloud compute/storage costs, and ensures 100% data fidelity through strict reconciliation.

## 🏗 Architecture Highlights

- **Event-Driven Orchestration:** Uses an Airflow `FileSensor` to actively monitor cloud storage drops (S3/Azure Blob). This completely eliminates race conditions associated with traditional cron-based schedules.
- **Cost-Optimized Staging:** Ingests raw data via Snowflake `COPY INTO` directly into **Transient Tables**, avoiding expensive Fail-safe and Time Travel storage overhead for ephemeral buffer data.
- **Atomic Upserts:** Merges data into the final target table using an idempotent `MERGE INTO` pattern matched on `trade_id`, seamlessly handling both historical inserts and updates.
- **Automated Validation:** Includes an independent Python-based reconciliation auditor that calculates aggregate checksums (row count, total volume, total price) to mathematically prove migration integrity.

## 📂 Repository Structure

```text
legacy-snowflake-migration/
├── dags/
│   └── trade_migration_dag.py     # Airflow event-driven orchestrator
├── helm/
│   └── custom_values.yaml         # K8s tuning for Airflow startup probes
├── sql/
│   ├── 01_init.sql                # DDL: Warehouse, Database, Schema setup
│   ├── 02_load_transient.sql      # DDL/DML: Staging tables and COPY INTO
│   └── 03_merge.sql               # DML: Atomic UPSERT to final target
├── mock_legacy_data.py            # Generates massive mock legacy CSV payload
├── reconcile_migration.py         # Independent data integrity auditor
├── Jenkinsfile                    # CI/CD pipeline definition
└── requirements.txt               # Python dependencies
```

## 🚀 Quickstart Guide

### 1. Generate Mock Legacy Data
Simulate the on-premises Hive metastore extract. This creates a `legacy_trades.csv` file in your root directory.
```bash
python mock_legacy_data.py
```

### 2. Initialize Snowflake
Bootstrap your Snowflake environment (requires SnowSQL).
Ensure you have `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and `SNOWFLAKE_PASSWORD` configured.
```bash
snowsql -a <account> -u <user> -f sql/01_init.sql
```

### 3. Deploy Airflow & Trigger DAG
Build the custom Docker image to include the required Snowflake providers and local DAGs.
```bash
docker build -t enterprise-registry.local/trade-migration:latest .
```
Deploy via Docker Compose or Helm (using `helm/custom_values.yaml`). Navigate to the Airflow UI, ensure the `snowflake_default` connection is securely configured, and unpause the `enterprise_trade_migration` DAG. The FileSensor will instantly detect `legacy_trades.csv` and begin the ingestion sequence.

### 4. Reconcile Migration
Mathematically prove that the legacy CSV safely migrated to the Snowflake target table.
```bash
python reconcile_migration.py
```

## 🛡 CI/CD & Code Quality
All commits are statically analyzed via `flake8` to enforce PEP-8 standards as defined in our `Jenkinsfile`. The pipeline is containerized into a production-ready Airflow image, capable of seamlessly deploying to a Kubernetes environment using KEDA for worker autoscaling.
