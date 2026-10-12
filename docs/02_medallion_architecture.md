# Medallion Architecture & Open Table Formats

## Table of Contents
1. [What is Medallion Architecture?](#1-what-is-medallion-architecture)
2. [The Three Layers](#2-the-three-layers)
3. [Why Three Layers?](#3-why-three-layers)
4. [Open Table Formats](#4-open-table-formats)
5. [Delta Lake vs Apache Hudi vs Apache Iceberg](#5-delta-lake-vs-apache-hudi-vs-apache-iceberg)
6. [This Project's Layer Design](#6-this-projects-layer-design)
7. [Data Flow End-to-End](#7-data-flow-end-to-end)

---

## 1. What is Medallion Architecture?

**Medallion architecture** (also called multi-hop architecture) is a data design pattern that
organizes data into progressively refined layers — named Bronze, Silver, and Gold after the
quality they represent.

```
                  Raw Data
                     │
                     ▼
              ┌─────────────┐
              │   BRONZE    │  Raw ingestion — as-is, minimal transformation
              │  (raw zone) │  "What came in?"
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │   SILVER    │  Cleansed, typed, enriched, deduplicated
              │ (clean zone)│  "What is the truth?"
              └──────┬──────┘
                     │
                     ▼
              ┌─────────────┐
              │    GOLD     │  Aggregated, business-ready
              │(serving zone│  "What does it mean?"
              └─────────────┘
```

Each layer serves a different consumer and a different purpose.
The pattern was popularized by Databricks but applies to any lakehouse implementation.

---

## 2. The Three Layers

### Bronze — The Raw Landing Zone

**Purpose:** Capture data exactly as it arrived from the source.

**Characteristics:**
- No transformation beyond adding audit metadata columns
- All values stored as strings (no type casting)
- Append-only — data is never deleted or modified
- Partitioned by load batch so you can trace every record to its source run
- If the source had bad data, Bronze has bad data — that is intentional

**What Bronze gives you:**
- A permanent, immutable record of what the source sent
- Ability to replay transformations if Silver/Gold logic changes
- Full audit trail — every row knows when it arrived, from which file, and which batch

**In this project:**
```
bronze_operator      ← operator.csv rows + _ingest_timestamp, _source_file, _load_batch
bronze_machine       ← machine.csv rows + metadata
bronze_material      ← material.csv rows + metadata
bronze_supplier      ← supplier.json rows (pivoted from key-value array) + metadata
bronze_operation_log ← operation_log.json rows (flattened from nested object) + metadata
```

---

### Silver — The Cleansed and Conformed Layer

**Purpose:** Apply business rules, type casting, deduplication, and history management.

**Characteristics:**
- Types are enforced (strings become integers, timestamps, decimals)
- Dimensions use SCD Type 2 — every change creates a new version; old version is expired
- Facts are append-only (operation logs are immutable events)
- Derived columns are computed (e.g., `duration_minutes`, `defect_rate`)
- Data is validated and cleaned

**What Silver gives you:**
- A single source of truth for each entity
- Full historical record of every dimension change
- Properly typed data ready for analytical queries
- Deduplication — each business key appears once as the current version

**In this project:**
```
silver_operator      ← SCD2: effective_from, effective_to, is_current, record_hash
silver_machine       ← SCD2
silver_material      ← SCD2
silver_supplier      ← SCD2 (composite key: supplier_id + supplier_machine_id)
silver_operation_log ← Append-only, typed, with duration_minutes and defect_rate derived
```

---

### Gold — The Business-Ready Serving Layer

**Purpose:** Aggregated, pre-joined, business-metric tables optimized for reporting.

**Characteristics:**
- Aggregated at the grain the business needs (daily, per-entity)
- Pre-computed metrics (counts, rates, averages, percentages)
- Overwritten daily (idempotent re-runs)
- Designed for direct consumption by dashboards, reports, or Athena ad-hoc queries

**What Gold gives you:**
- Fast, simple SQL queries for business users
- Pre-computed expensive aggregations
- Clean, named columns with business meaning

**In this project:**
```
gold_operation_summary     ← daily: total ops, total qty, defects, defect rate, avg quality
gold_defect_analysis       ← per defect type: count, percentage, rank
gold_supplier_performance  ← per supplier: avg rating, lead time, quality score
```

---

## 3. Why Three Layers?

### Separation of Concerns

Each layer has a single responsibility. If a transformation bug is found in Silver,
you fix the Silver job and re-run it from Bronze — without re-ingesting from the source.

```
Source broken?  → Re-ingest Bronze from source file
Silver logic wrong? → Re-run Silver from Bronze (source unchanged)
Gold report wrong?  → Re-run Gold from Silver (dims unchanged)
```

### Resilience

Bronze acts as a permanent backup of raw data. Even if you accidentally delete Silver and Gold,
you can recreate them entirely from Bronze.

### Performance

Gold tables are pre-aggregated for the most common query patterns. End users get sub-second
response times because the heavy joins and aggregations have already been done.

### Collaboration

Different teams own different layers:
- **Data engineering** owns Bronze and Silver pipelines
- **Analytics engineering** owns Gold layer definitions
- **Business analysts** consume Gold via Athena or BI tools

---

## 4. Open Table Formats

An **open table format** is a specification that adds a metadata layer on top of regular
files in object storage (S3, GCS, ADLS), giving them properties normally associated
with database tables.

```
Without open table format:
  S3 bucket/prefix/
    ├── part-00000.parquet
    ├── part-00001.parquet      ← Just files. No ACID. No schema. No history.
    └── part-00002.parquet

With Apache Iceberg:
  S3 bucket/prefix/
    ├── data/
    │   ├── part-00000.parquet
    │   └── part-00001.parquet
    └── metadata/
        ├── v1.metadata.json    ← Schema, partitioning, snapshot history
        ├── v2.metadata.json
        ├── snap-001.avro       ← Snapshot manifest
        └── snap-002.avro
```

The metadata layer enables:

| Feature | Without Format | With Iceberg |
|---------|---------------|-------------|
| ACID transactions | No | Yes |
| Schema evolution | No | Yes (add, rename, drop, reorder columns) |
| Time travel | No | Yes (query any past snapshot) |
| Partition evolution | No | Yes (change partitions without rewrite) |
| Row-level updates | No | Yes (MERGE, UPDATE, DELETE) |
| Concurrent writes | Unsafe | Safe |
| Statistics | No | Yes (min/max per column, per file) |

---

## 5. Delta Lake vs Apache Hudi vs Apache Iceberg

There are three major open table formats. Here is how they compare:

| Feature | Delta Lake | Apache Hudi | Apache Iceberg |
|---------|-----------|-------------|----------------|
| Origin | Databricks | Uber | Netflix |
| License | Apache 2.0 | Apache 2.0 | Apache 2.0 |
| AWS Glue support | Via connector | Via connector | Native (built-in) |
| Athena support | Via manifest | Via manifest | Native |
| ACID | Yes | Yes | Yes |
| Time travel | Yes | Limited | Yes (full) |
| Schema evolution | Yes | Limited | Full |
| Partition evolution | No (rewrite) | No | Yes (hidden partitions) |
| Primary key required | No | Yes (for MOR) | No |
| MERGE INTO | Yes | Yes | Yes |
| AWS Lake Formation | Partial | Partial | Full |

**Why we chose Apache Iceberg:**

1. **Native AWS support** — AWS Glue 5.0 and Athena support Iceberg without any connectors.
   Lake Formation has full governance support for Iceberg tables.

2. **No primary key requirement** — Unlike Hudi, Iceberg does not require you to define
   a primary key at the table level. Our SCD2 logic manages keys in PySpark.

3. **Full schema and partition evolution** — We can add columns or change partitioning
   without rewriting data.

4. **Format version 2** — Iceberg v2 supports row-level deletes (position deletes and
   equality deletes), which is what `MERGE INTO` uses internally.

---

## 6. This Project's Layer Design

```
Layer   │ Tables                    │ Format  │ Partition              │ Write Mode
────────┼───────────────────────────┼─────────┼────────────────────────┼────────────
Bronze  │ bronze_operator           │ Iceberg │ _load_batch            │ Append
        │ bronze_machine            │ Iceberg │ _load_batch            │ Append
        │ bronze_material           │ Iceberg │ _load_batch            │ Append
        │ bronze_supplier           │ Iceberg │ _load_batch            │ Append
        │ bronze_operation_log      │ Iceberg │ _load_batch            │ Append
────────┼───────────────────────────┼─────────┼────────────────────────┼────────────
Silver  │ silver_operator           │ Iceberg │ is_current             │ SCD2 Merge
        │ silver_machine            │ Iceberg │ is_current             │ SCD2 Merge
        │ silver_material           │ Iceberg │ is_current             │ SCD2 Merge
        │ silver_supplier           │ Iceberg │ is_current             │ SCD2 Merge
        │ silver_operation_log      │ Iceberg │ days(start_time)       │ Append
────────┼───────────────────────────┼─────────┼────────────────────────┼────────────
Gold    │ gold_operation_summary    │ Iceberg │ report_date            │ Overwrite
        │ gold_defect_analysis      │ Iceberg │ report_date            │ Overwrite
        │ gold_supplier_performance │ Iceberg │ report_date            │ Overwrite
```

### Iceberg Format Version 2

All tables are created with `'format-version'='2'`. This enables:
- **Row-level deletes** used by `MERGE INTO` in the SCD2 Silver jobs
- **Equality deletes** for more efficient delete operations
- **Full ACID** guarantees across all write operations

---

## 7. Data Flow End-to-End

```
Step 1: Raw data uploaded to S3
  s3://bucket/raw/data_1/operator/operator.csv
  s3://bucket/raw/data_1/machine/machine.csv
  ...

Step 2: Bronze ingestion (5 parallel Glue jobs)
  CSV/JSON → Spark DataFrame → add audit cols → Iceberg append
  Result: bronze_operator, bronze_machine, ... in mfg_bronze_db

Step 3: Silver transformation (1 Glue job, sequential)
  For each dimension:
    bronze_xxx.filter(_load_batch='data_1')
    → deduplicate by PK
    → compute record_hash of tracked columns
    → if initial: write all rows as current (is_current=true)
    → if incremental:
        join with existing current rows
        expired changed rows (MERGE INTO: is_current=false, effective_to=now)
        insert new/changed rows (is_current=true, effective_from=now)

  For operation_log:
    bronze_operation_log.filter(_load_batch='data_1')
    → type cast all columns
    → compute duration_minutes, defect_rate
    → append to silver_operation_log

Step 4: Gold aggregation (1 Glue job)
  silver_operation_log
    join silver_operator, silver_machine, silver_material, silver_supplier
    aggregate by report_date
  → overwrite gold_operation_summary, gold_defect_analysis, gold_supplier_performance

Step 5: Query via Athena
  SELECT * FROM mfg_gold_db.gold_operation_summary WHERE report_date = '2024-01-15'
```

Continue reading:
- [03_apache_iceberg.md](03_apache_iceberg.md) — Deep dive on Iceberg internals
- [04_scd_type2.md](04_scd_type2.md) — How SCD2 works with Iceberg MERGE INTO
