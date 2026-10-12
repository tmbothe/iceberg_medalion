# Manufacturing Lakehouse — Apache Iceberg Medallion Architecture on AWS

> A production-style data lakehouse pipeline that processes manufacturing assembly-line data through
> Bronze → Silver → Gold layers using Apache Iceberg on S3, orchestrated with AWS Step Functions
> and transformed by AWS Glue 5.0 (PySpark).

---

## The Problem

A small manufacturing company runs an assembly line producing goods across multiple workstations.
Operators, machines, materials, and suppliers each generate operational data. The company needs to:

- Store and query all historical changes to dimensions (operators, machines, materials, suppliers)
- Track every assembly-line operation with defect rates and quality scores
- Generate daily reports on production efficiency, defect patterns, and supplier performance
- Do this **cheaply** — no Redshift or Snowflake; use S3 + open table format instead

See [docs/01_problem_statement.md](docs/01_problem_statement.md) for the full business context.

---

## Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────┐
│                         S3 Bucket                               │
│                                                                 │
│  raw/data_1/          raw/data_2/                               │
│  (CSV + JSON)         (CSV + JSON)                              │
│        │                    │                                   │
│        └──────────┬─────────┘                                   │
│                   ▼  [5 Glue jobs, parallel]                    │
│         ┌─────────────────────┐                                 │
│         │   BRONZE  layer     │  Iceberg, all-string, partitioned│
│         │  mfg_bronze_db      │  by _load_batch                 │
│         └──────────┬──────────┘                                 │
│                    ▼  [1 Glue job — SCD2 + append]              │
│         ┌─────────────────────┐                                 │
│         │   SILVER  layer     │  Iceberg, typed, SCD2 history   │
│         │  mfg_silver_db      │  + append fact table            │
│         └──────────┬──────────┘                                 │
│                    ▼  [1 Glue job — aggregate]                  │
│         ┌─────────────────────┐                                 │
│         │    GOLD   layer     │  Iceberg, daily reports         │
│         │  mfg_gold_db        │  queryable via Athena           │
│         └─────────────────────┘                                 │
└─────────────────────────────────────────────────────────────────┘
             Orchestrated by AWS Step Functions
             Governed by AWS Lake Formation
             Catalogued in AWS Glue Data Catalog
```

---

## Documentation

| # | Page | What it covers |
|---|------|---------------|
| 1 | [Problem Statement](docs/01_problem_statement.md) | Business context, assembly-line domain, why lakehouse |
| 2 | [Medallion Architecture](docs/02_medallion_architecture.md) | Bronze/Silver/Gold pattern, open table formats |
| 3 | [Apache Iceberg](docs/03_apache_iceberg.md) | What Iceberg is, ACID, time travel, partitioning, snapshots |
| 4 | [SCD Type 2](docs/04_scd_type2.md) | Slowly Changing Dimensions, Iceberg MERGE INTO pattern |
| 5 | [AWS Prerequisites](docs/05_aws_prerequisites.md) | Account, IAM, S3, CLI setup |
| 6 | [Lake Formation](docs/06_lake_formation.md) | Data lake admin, databases, users, permissions |
| 7 | [Glue Data Catalog](docs/07_glue_catalog.md) | Databases, tables, schema, Iceberg registration |
| 8 | [Bronze Layer](docs/08_bronze_layer.md) | Raw → Bronze ingestion for all 5 entities |
| 9 | [Silver Layer](docs/09_silver_layer.md) | SCD2 dimensions + append fact, type casting |
| 10 | [Gold Layer](docs/10_gold_layer.md) | Aggregated reports, Athena queries |
| 11 | [Step Functions Pipeline](docs/11_step_functions_pipeline.md) | Orchestration, ASL walkthrough, default params |
| 12 | [Data Lineage](docs/12_data_lineage.md) | Lineage tracking, Iceberg snapshots, time travel |
| 13 | [Deployment Guide](docs/13_deployment_guide.md) | Step-by-step from zero to running pipeline |
| 14 | [Validation & Testing](docs/14_validation_and_testing.md) | Athena queries to verify each layer |

---

## Project Structure

```
iceberg_medalion/
├── README.md
├── .gitignore
├── data/
│   ├── data_1/          # Initial load seed data
│   └── data_2/          # Incremental load seed data (SCD2 changes + new records)
├── docs/                # 14 documentation pages
├── glue_jobs/           # PySpark ETL scripts (numbered in execution order)
│   ├── 00_cleanup_tables.py
│   ├── 01_raw_to_bronze_operator.py
│   ├── 02_raw_to_bronze_machine.py
│   ├── 03_raw_to_bronze_material.py
│   ├── 04_raw_to_bronze_supplier.py
│   ├── 05_raw_to_bronze_operation_log.py
│   ├── 06_bronze_to_silver.py
│   └── 07_silver_to_gold.py
├── infrastructure/
│   ├── iam/lf-access.json
│   └── stepfunctions/pipeline_definition.json
└── scripts/             # Run in numbered order for fresh deployment
    ├── 1_upload_raw_data.sh
    ├── 2_create_glue_catalog.sh
    ├── 3_sync_glue_jobs.sh
    └── 4_deploy_stepfunctions.sh
```

---

## Quick Start

```bash
# Prerequisites: AWS CLI configured, S3 bucket exists, IAM role exists
# See docs/13_deployment_guide.md for full setup instructions

cd iceberg_medalion

bash scripts/1_upload_raw_data.sh       # Upload seed data + scripts to S3
bash scripts/2_create_glue_catalog.sh   # Create Glue databases + jobs
bash scripts/3_sync_glue_jobs.sh        # Sync ETL scripts → S3
bash scripts/4_deploy_stepfunctions.sh  # Deploy orchestration pipeline
```

**Trigger the pipeline** (uses incremental defaults):
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline" \
  --input '{}'
```

**Initial load** (first run with data_1):
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:<ACCOUNT_ID>:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline" \
  --input '{"load_batch": "data_1", "load_type": "initial"}'
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Storage | Amazon S3 |
| Table Format | Apache Iceberg 1.x (format-version 2) |
| ETL Engine | AWS Glue 5.0 (PySpark 3.x) |
| Data Catalog | AWS Glue Data Catalog |
| Governance | AWS Lake Formation |
| Orchestration | AWS Step Functions (Standard Workflow) |
| Ad-hoc Query | Amazon Athena |
| IAM | AWS IAM + Lake Formation permissions |

---

## Key Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Table format | Apache Iceberg | ACID, time travel, schema evolution, Glue/Athena native support |
| Storage | S3 + Iceberg vs Redshift | Cost — no cluster to maintain |
| SCD strategy | SCD Type 2 | Preserve full history of dimension changes |
| Bronze contract | All strings, no parsing | Decouple ingestion from transformation; never lose raw data |
| Bronze partitioning | `_load_batch` | Idempotent re-runs; easy to filter by batch |
| Silver dims partitioning | `is_current` | Fast filter for "give me current records only" |
| Gold pattern | Overwrite by `report_date` | Idempotent daily re-runs |

---

## License

MIT
