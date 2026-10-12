# Data Lineage

## Table of Contents
1. [What is Data Lineage?](#1-what-is-data-lineage)
2. [Lineage in This Project: End-to-End Map](#2-lineage-in-this-project-end-to-end-map)
3. [Table-Level Lineage](#3-table-level-lineage)
4. [Column-Level Lineage](#4-column-level-lineage)
5. [Tracking Lineage via Audit Columns](#5-tracking-lineage-via-audit-columns)
6. [Iceberg Snapshot Lineage and Time Travel](#6-iceberg-snapshot-lineage-and-time-travel)
7. [AWS Glue Data Catalog Lineage](#7-aws-glue-data-catalog-lineage)
8. [Querying Lineage at Runtime](#8-querying-lineage-at-runtime)
9. [SCD2 Version Lineage](#9-scd2-version-lineage)
10. [Extending with a Lineage Tool](#10-extending-with-a-lineage-tool)

---

## 1. What is Data Lineage?

**Data lineage** is the ability to trace a piece of data from its origin through every
transformation it underwent to reach its final form.

It answers questions like:
- "Where did this number come from?"
- "Which source file contains the raw version of this row?"
- "If I change the Silver transformation logic, which Gold tables are affected?"
- "When was this record last modified and by which job?"
- "What did this table look like before the last pipeline run?"

Good lineage enables:
- **Debugging** — trace errors back to their source
- **Impact analysis** — understand what downstream tables are affected by a change
- **Audit** — demonstrate to auditors that data is accurate and traceable
- **Compliance** — meet regulatory requirements for data traceability

---

## 2. Lineage in This Project: End-to-End Map

```
┌───────────────────────────────────────────────────────────────────────────┐
│  SOURCE FILES (S3 raw/)                                                   │
│                                                                           │
│  raw/data_1/operator/operator.csv ──────────────────────────────────┐    │
│  raw/data_1/machine/machine.csv  ───────────────────────────────┐   │    │
│  raw/data_1/material/material.csv ──────────────────────────┐   │   │    │
│  raw/data_1/supplier/supplier.json ─────────────────────┐   │   │   │    │
│  raw/data_1/operation_log/operation_log.json ───────┐   │   │   │   │    │
│                                                     │   │   │   │   │    │
│  (data_2 same structure)                            │   │   │   │   │    │
└─────────────────────────────────────────────────────┼───┼───┼───┼───┼────┘
                                                      │   │   │   │   │
                                         Glue job:    │   │   │   │   │
                                         01–05        │   │   │   │   │
                                                      ▼   ▼   ▼   ▼   ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  BRONZE (mfg_bronze_db)                                                   │
│                                                                           │
│  bronze_operation_log ◄─────────────────────────────────────────────────┘│
│  bronze_supplier      ◄────────────────────────────────────────────────┐  │
│  bronze_material      ◄───────────────────────────────────────────┐    │  │
│  bronze_machine       ◄──────────────────────────────────────┐    │    │  │
│  bronze_operator      ◄─────────────────────────────────┐    │    │    │  │
└──────────────────────────────────────────────────────────┼────┼────┼────┼──┘
                                                           │    │    │    │
                                          Glue job: 06     │    │    │    │
                                                           ▼    ▼    ▼    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  SILVER (mfg_silver_db)                                                   │
│                                                                           │
│  silver_operator      ◄─────────────── bronze_operator                   │
│  silver_machine       ◄─────────────── bronze_machine                    │
│  silver_material      ◄─────────────── bronze_material                   │
│  silver_supplier      ◄─────────────── bronze_supplier                   │
│  silver_operation_log ◄─────────────── bronze_operation_log              │
└──────────────────────────────────────────────────────────────────────────┘
                                                           │
                                          Glue job: 07     │
                                                           ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  GOLD (mfg_gold_db)                                                       │
│                                                                           │
│  gold_operation_summary ◄──────────────── silver_operation_log           │
│  gold_defect_analysis   ◄──────────────── silver_operation_log           │
│  gold_supplier_performance ◄─ silver_operation_log                       │
│                               + silver_machine                           │
│                               + silver_supplier                          │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Table-Level Lineage

| Target Table | Source Tables | Transformation | Glue Job |
|-------------|--------------|----------------|----------|
| `bronze_operator` | `raw/*/operator/operator.csv` | Add audit cols | `01_raw_to_bronze_operator.py` |
| `bronze_machine` | `raw/*/machine/machine.csv` | Add audit cols | `02_raw_to_bronze_machine.py` |
| `bronze_material` | `raw/*/material/material.csv` | Add audit cols | `03_raw_to_bronze_material.py` |
| `bronze_supplier` | `raw/*/supplier/supplier.json` | Pivot key-value array + audit cols | `04_raw_to_bronze_supplier.py` |
| `bronze_operation_log` | `raw/*/operation_log/operation_log.json` | Flatten nested object + audit cols | `05_raw_to_bronze_operation_log.py` |
| `silver_operator` | `bronze_operator` | SCD2 merge, record_hash | `06_bronze_to_silver.py` |
| `silver_machine` | `bronze_machine` | SCD2 merge | `06_bronze_to_silver.py` |
| `silver_material` | `bronze_material` | SCD2 merge | `06_bronze_to_silver.py` |
| `silver_supplier` | `bronze_supplier` | SCD2 merge + synthetic PK | `06_bronze_to_silver.py` |
| `silver_operation_log` | `bronze_operation_log` | Type cast + derived cols | `06_bronze_to_silver.py` |
| `gold_operation_summary` | `silver_operation_log` | GROUP BY report_date | `07_silver_to_gold.py` |
| `gold_defect_analysis` | `silver_operation_log` | GROUP BY defect_type + window rank | `07_silver_to_gold.py` |
| `gold_supplier_performance` | `silver_operation_log` + `silver_machine` + `silver_supplier` | JOIN + GROUP BY supplier | `07_silver_to_gold.py` |

---

## 4. Column-Level Lineage

### bronze_operator → silver_operator

| Silver Column | Source | Transformation |
|---------------|--------|----------------|
| `operator_id` | `bronze_operator.operator_id` | Direct passthrough |
| `operator_name` | `bronze_operator.operator_name` | Direct passthrough |
| `workstation_id` | `bronze_operator.workstation_id` | Direct passthrough |
| `skill_level` | `bronze_operator.skill_level` | Direct passthrough |
| `hire_date` | `bronze_operator.hire_date` | Direct passthrough |
| `shift` | `bronze_operator.shift` | Direct passthrough |
| `status` | `bronze_operator.status` | Direct passthrough |
| `effective_from` | — | Set to `current_timestamp()` on write |
| `effective_to` | — | NULL on insert; set to `current_timestamp()` on SCD2 expiry |
| `is_current` | — | TRUE on insert; FALSE on SCD2 expiry |
| `record_hash` | `operator_name`, `workstation_id`, `skill_level`, `shift`, `status` | `md5(concat_ws('\|', cols))` |
| `_created_at` | — | Set to `current_timestamp()` on first insert |
| `_updated_at` | — | Set on insert and on expiry |

### bronze_operation_log → silver_operation_log

| Silver Column | Source | Transformation |
|---------------|--------|----------------|
| `oplog_id` | `bronze_operation_log.oplog_id` | Direct |
| `start_time` | `bronze_operation_log.start_time` (STRING) | `to_timestamp()` |
| `end_time` | `bronze_operation_log.end_time` (STRING) | `to_timestamp()` |
| `duration_minutes` | `start_time`, `end_time` | `(unix_ts(end) - unix_ts(start)) / 60.0` |
| `quantity_processed` | `bronze_operation_log.quantity_processed` (STRING) | `.cast(IntegerType())` |
| `defects_found` | `bronze_operation_log.defects_found` (STRING) | `.cast(IntegerType())` |
| `defect_rate` | `defects_found`, `quantity_processed` | `defects / qty * 100` if qty > 0 |
| `quality_score` | `bronze_operation_log.quality_score` (STRING) | `.cast(DecimalType(5,2))` |

### silver_operation_log → gold_operation_summary

| Gold Column | Source | Transformation |
|-------------|--------|----------------|
| `report_date` | `silver_operation_log.start_time` | `to_date(start_time)` |
| `total_operations` | `silver_operation_log.oplog_id` | `count()` |
| `total_quantity` | `silver_operation_log.quantity_processed` | `sum()` |
| `total_defects` | `silver_operation_log.defects_found` | `sum()` |
| `defect_rate_pct` | `total_defects`, `total_quantity` | `sum(defects)/sum(qty)*100` |
| `avg_quality_score` | `silver_operation_log.quality_score` | `avg()` |

---

## 5. Tracking Lineage via Audit Columns

Every Bronze row contains three audit columns that make lineage traceable at the row level:

```sql
-- Find the source file for any Bronze row
SELECT
    operator_id,
    operator_name,
    _source_file,      ← exact S3 file path
    _load_batch,       ← which batch
    _ingest_timestamp  ← when it was loaded
FROM mfg_bronze_db.bronze_operator
WHERE operator_id = 'OP002';
```

```
operator_id | operator_name | _source_file                                          | _load_batch | _ingest_timestamp
OP002       | Bob Smith     | s3://mfg-lakehouse-.../raw/data_1/operator/operator.csv | data_1      | 2024-01-15 10:00:00
OP002       | Bob Smith     | s3://mfg-lakehouse-.../raw/data_2/operator/operator.csv | data_2      | 2024-02-01 09:00:00
```

You can trace from Gold → Silver → Bronze → source file:

```sql
-- Trace a Gold metric back to its Bronze source
-- Step 1: Find the operation that contributed to the defect rate
SELECT
    ol.oplog_id,
    ol.start_time,
    ol.defects_found,
    ol.defect_rate
FROM mfg_silver_db.silver_operation_log ol
WHERE CAST(ol.start_time AS DATE) = DATE '2024-01-15'
ORDER BY ol.defects_found DESC
LIMIT 3;

-- Step 2: Find the Bronze row for that oplog_id
SELECT
    oplog_id,
    _source_file,
    _load_batch,
    _ingest_timestamp
FROM mfg_bronze_db.bronze_operation_log
WHERE oplog_id = 'OL001';

-- Step 3: Open that source file in S3 to see the original raw record
-- s3://mfg-lakehouse-.../raw/data_1/operation_log/operation_log.json
```

---

## 6. Iceberg Snapshot Lineage and Time Travel

Iceberg automatically maintains a complete history of every write operation via snapshots.
This provides built-in lineage at the data-version level.

### View All Snapshots

```sql
-- In Athena: list all snapshots for a table
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary
FROM mfg_silver_db."silver_operator$snapshots"
ORDER BY committed_at;
```

```
snapshot_id   | committed_at             | operation | summary
12345678901   | 2024-01-15 10:05:00 UTC  | append    | {"added-records": "8", ...}
98765432101   | 2024-02-01 09:12:00 UTC  | overwrite | {"deleted-records": "3", "added-records": "8", ...}
```

### Query a Previous Snapshot (Time Travel)

```sql
-- What did silver_operator look like before the incremental load?
SELECT *
FROM mfg_silver_db.silver_operator
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-16 00:00:00'
WHERE operator_id = 'OP002';
-- Returns: the original data_1 version of OP002
```

### View Data File Lineage

```sql
-- See all data files in the current snapshot
SELECT
    file_path,
    record_count,
    file_size_in_bytes,
    partition
FROM mfg_silver_db."silver_operator$files";
```

### View Table History

```sql
-- Full history of operations on the table
SELECT *
FROM mfg_silver_db."silver_operator$history"
ORDER BY made_current_at;
```

---

## 7. AWS Glue Data Catalog Lineage

As of the current version of this project, AWS Glue's built-in lineage feature is not
explicitly enabled. AWS Glue does however track some lineage automatically:

### Glue Job Run History

Every Glue job run is logged in the Glue console:
- Job name
- Run ID
- Start/end time
- Parameters used (`--LOAD_BATCH`, `--LOAD_TYPE`, etc.)
- Success/failure + error message

```bash
# View recent runs of the Silver job
aws glue get-job-runs \
    --job-name mfg-bronze-to-silver \
    --query 'JobRuns[].{
        RunId:Id,
        Status:JobRunState,
        Start:StartedOn,
        Batch:Arguments."--LOAD_BATCH",
        Type:Arguments."--LOAD_TYPE"
    }'
```

### Connecting Table Writes to Job Runs

By correlating the Iceberg snapshot `committed_at` timestamp with the Glue job run
`StartedOn`/`CompletedOn` timestamps, you can identify exactly which job run produced
each snapshot.

---

## 8. Querying Lineage at Runtime

### Which Batch Produced a Silver Row?

Silver rows don't have `_load_batch` directly, but you can trace back through Bronze:

```sql
-- Find which load batch introduced OP002's current version
SELECT
    s.operator_id,
    s.effective_from,
    b._load_batch,
    b._source_file,
    b._ingest_timestamp
FROM mfg_silver_db.silver_operator s
JOIN mfg_bronze_db.bronze_operator b
  ON s.operator_id = b.operator_id
WHERE s.operator_id = 'OP002'
  AND s.is_current = true
  AND ABS(EXTRACT(EPOCH FROM (s.effective_from - b._ingest_timestamp))) < 60
ORDER BY s.effective_from DESC
LIMIT 1;
```

### Which Operations Contributed to a Gold Metric?

```sql
-- Which operations drove the defect_rate in gold_operation_summary for 2024-01-15?
SELECT
    ol.oplog_id,
    ol.operator_id,
    ol.machine_id,
    ol.defects_found,
    ol.defect_rate,
    ol.defect_type
FROM mfg_silver_db.silver_operation_log ol
WHERE CAST(ol.start_time AS DATE) = DATE '2024-01-15'
ORDER BY ol.defects_found DESC;
```

---

## 9. SCD2 Version Lineage

The SCD2 pattern creates its own form of lineage — a complete history of every version
of every dimension record:

```sql
-- Full version history of supplier SUP001~M001
SELECT
    supplier_pk,
    company_name,
    country,
    lead_time_days,
    rating,
    effective_from,
    effective_to,
    is_current,
    _updated_at
FROM mfg_silver_db.silver_supplier
WHERE supplier_id = 'SUP001'
ORDER BY effective_from;
```

```
supplier_pk | company_name  | rating | effective_from | effective_to | is_current
SUP001~M001 | TechSupply Co | 4.5    | 2024-01-15    | 2024-02-01   | FALSE    ← old version
SUP001~M001 | TechSupply Co | 4.7    | 2024-02-01    | NULL         | TRUE     ← current
```

This lineage is essential for answering: "What was this supplier's rating when this
operation was performed?" — just join with the version that was active at that time.

---

## 10. Extending with a Lineage Tool

For a production deployment, consider adding a dedicated lineage tool:

### Option 1: AWS Glue Data Quality + DataBrew
- AWS native
- Provides data quality lineage (not full column-level lineage)
- No additional infrastructure

### Option 2: Apache Atlas
- Open source metadata and governance framework
- Full column-level lineage
- Integrates with Hive/Spark (requires custom hook)
- Requires a separate server

### Option 3: OpenLineage + Marquez
- Open standard for lineage (OpenLineage) + visualization (Marquez)
- Spark integration via `openlineage-spark` library
- Add to Glue jobs:
  ```python
  # In Glue job default arguments:
  "--conf": "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener",
  "--conf": "spark.openlineage.transport.type=http",
  "--conf": "spark.openlineage.transport.url=http://marquez-server:5000"
  ```
- Automatically captures table-level lineage from Spark SQL operations
- Marquez provides a web UI showing the lineage graph

### Option 4: DataHub
- LinkedIn's open source metadata platform
- Deep integration with Spark, Glue, Athena
- Column-level lineage
- Requires Kubernetes deployment
