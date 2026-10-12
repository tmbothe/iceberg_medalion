# Apache Iceberg — Deep Dive

## Table of Contents
1. [What is Apache Iceberg?](#1-what-is-apache-iceberg)
2. [Iceberg Architecture: Files, Manifests, Metadata](#2-iceberg-architecture-files-manifests-metadata)
3. [Snapshots and Time Travel](#3-snapshots-and-time-travel)
4. [ACID Transactions](#4-acid-transactions)
5. [Schema Evolution](#5-schema-evolution)
6. [Partition Evolution and Hidden Partitioning](#6-partition-evolution-and-hidden-partitioning)
7. [Row-Level Operations: MERGE, UPDATE, DELETE](#7-row-level-operations-merge-update-delete)
8. [Iceberg in AWS: Glue + Athena Integration](#8-iceberg-in-aws-glue--athena-integration)
9. [Iceberg Table Properties Used in This Project](#9-iceberg-table-properties-used-in-this-project)
10. [Iceberg Catalog: What It Is and Why It Matters](#10-iceberg-catalog-what-it-is-and-why-it-matters)

---

## 1. What is Apache Iceberg?

Apache Iceberg is an **open table format** for large analytic datasets on distributed storage.
Originally created at Netflix in 2017 to solve production data lake problems at petabyte scale,
it became an Apache Software Foundation top-level project in 2020.

Iceberg is **not a storage engine** and **not a query engine**. It is a **specification** —
a set of rules for how to organize files in object storage and track metadata — so that
any compatible engine (Spark, Trino, Flink, Athena, Hive, DuckDB) can read and write
the same table reliably and consistently.

```
                  ┌──────────────────────────────────────────┐
                  │        Query Engines (readers/writers)    │
                  │   Spark │ Athena │ Trino │ Flink │ DuckDB │
                  └────────────────────┬─────────────────────┘
                                       │  Iceberg Table API
                  ┌────────────────────▼─────────────────────┐
                  │           Apache Iceberg Library          │
                  │   (metadata management, snapshot tracking)│
                  └────────────────────┬─────────────────────┘
                                       │  File I/O API
                  ┌────────────────────▼─────────────────────┐
                  │        Object Storage (S3, GCS, ADLS)     │
                  │   Parquet files │ ORC files │ Avro files  │
                  └──────────────────────────────────────────┘
```

---

## 2. Iceberg Architecture: Files, Manifests, Metadata

An Iceberg table on S3 consists of three layers of files:

```
s3://bucket/silver/operator/
├── data/
│   ├── is_current=true/
│   │   └── 00000-1-abc.parquet   ← data files (Parquet by default)
│   └── is_current=false/
│       └── 00000-2-def.parquet
└── metadata/
    ├── v1.metadata.json          ← table metadata (schema, partitions, snapshots list)
    ├── v2.metadata.json
    ├── snap-12345-manifest-list.avro  ← manifest list (one entry per manifest file)
    └── manifests/
        └── manifest-001.avro    ← manifest file (one entry per data file)
```

### Data Files
Plain Parquet (or ORC/Avro) files containing the actual row data. Nothing special —
any Parquet reader can open them.

### Manifest Files
An Avro file that lists a set of data files along with:
- The file path
- Partition values (so queries can skip irrelevant files)
- Column statistics (min/max values per column, null counts)
- Whether the file contains added or deleted rows

### Manifest List (Snapshot)
Each **snapshot** has a manifest list — an Avro file that points to all the manifest files
for that snapshot. A snapshot represents the state of the table at a point in time.

### Table Metadata
A JSON file with:
- The current schema (column names, types, IDs)
- All known snapshots (history)
- Partition spec (how data is partitioned)
- Sort order
- Properties (format-version, compaction settings, etc.)

The **current snapshot pointer** in the metadata file is what makes atomic commits possible:
a write atomically swaps the metadata pointer from the old snapshot to the new one.

```
Write Operation:
  1. Write new data files to S3
  2. Write new manifest file listing those data files
  3. Write new manifest list (snapshot) pointing to new + existing manifests
  4. Write new metadata.json pointing to the new snapshot
  5. Atomically update the catalog (Glue Data Catalog) to point to the new metadata.json
  → If step 5 fails: old metadata.json still valid, no partial state visible to readers
```

---

## 3. Snapshots and Time Travel

Every write to an Iceberg table creates a new **snapshot** — an immutable, named point-in-time
view of the table. Old snapshots are preserved until explicitly expired.

```
Timeline of snapshots for silver_operator:

  Snapshot 1 (initial load, data_1)
  ├── 10 rows: OP001–OP010, all is_current=true

  Snapshot 2 (incremental load, data_2 — expire changed rows)
  ├── 10 rows from snapshot 1, but OP002/OP003/OP007 now is_current=false

  Snapshot 3 (incremental load, data_2 — insert new current rows)
  ├── Snapshot 2 + 3 new rows for OP002/OP003/OP007 (new versions, is_current=true)
  └── 2 new rows for OP009/OP010
```

### Querying Historical Snapshots

```sql
-- Query the table as it looked at a specific timestamp
SELECT * FROM glue_catalog.mfg_silver_db.silver_operator
TIMESTAMP AS OF TIMESTAMP '2024-01-15 12:00:00';

-- Query a specific snapshot by ID
SELECT * FROM glue_catalog.mfg_silver_db.silver_operator
VERSION AS OF 1234567890;

-- List all snapshots (Iceberg metadata table)
SELECT * FROM glue_catalog.mfg_silver_db.silver_operator.snapshots;

-- See the history of changes
SELECT * FROM glue_catalog.mfg_silver_db.silver_operator.history;
```

### In Athena
```sql
-- Time travel in Athena
SELECT * FROM mfg_silver_db.silver_operator
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 12:00:00';
```

---

## 4. ACID Transactions

Iceberg provides **serializable isolation** for reads and writes.

| Property | What it means |
|----------|---------------|
| **Atomicity** | A write either fully succeeds or has no effect. No partial tables. |
| **Consistency** | The table always moves from one valid state to another. |
| **Isolation** | Concurrent readers see a consistent snapshot; writers do not block readers. |
| **Durability** | Once committed, a write is permanent (stored in S3). |

### Optimistic Concurrency Control
Iceberg uses **optimistic concurrency** — multiple writers can work simultaneously, but
the final commit checks for conflicts. If two writers try to update the same rows, one will
succeed and the other will retry.

This is particularly important for the SCD2 `MERGE INTO` operations — Iceberg ensures that
expiring old rows and inserting new rows is an atomic operation.

---

## 5. Schema Evolution

Iceberg tracks column IDs internally (not by position or name), so schema changes never
cause data corruption.

```sql
-- Add a new column (safe — old files return NULL for the new column)
ALTER TABLE glue_catalog.mfg_silver_db.silver_operator
ADD COLUMN manager_id STRING;

-- Rename a column (safe — Iceberg maps by column ID, not name)
ALTER TABLE glue_catalog.mfg_silver_db.silver_operator
RENAME COLUMN workstation_id TO station_id;

-- Drop a column (safe — Iceberg marks it deleted in metadata, old files unaffected)
ALTER TABLE glue_catalog.mfg_silver_db.silver_operator
DROP COLUMN status;

-- Change a column type (allowed for safe upcasts: int→long, float→double, etc.)
ALTER TABLE glue_catalog.mfg_silver_db.silver_operator
ALTER COLUMN operator_id TYPE BIGINT;
```

**Why this matters:** Traditional Parquet lakes break on schema changes because columns are
read by position or name, and missing columns cause errors. Iceberg's ID-based tracking
means you can safely evolve schemas without rewriting historical data.

---

## 6. Partition Evolution and Hidden Partitioning

### Hidden Partitioning
In traditional partitioned tables (Hive-style), users must filter by the partition column
explicitly: `WHERE _load_batch = 'data_1'`. If you forget, Spark reads all partitions.

Iceberg uses **hidden partitioning** — Iceberg tracks which files belong to which partition
values in the manifest files, and automatically prunes files without requiring you to
filter by a specific column name.

For example, with `PARTITIONED BY (days(start_time))`:
- Files are organized by date computed from `start_time`
- A query `WHERE start_time BETWEEN '2024-01-15' AND '2024-01-17'` automatically reads
  only the files for those 3 days — without a `WHERE _date = '2024-01-15'` filter

### Partition Transforms
Iceberg supports transforms on partition columns:

| Transform | Example | Effect |
|-----------|---------|--------|
| `identity` | `PARTITIONED BY (is_current)` | Partition by value as-is |
| `days` | `PARTITIONED BY (days(start_time))` | Partition by day from timestamp |
| `months` | `PARTITIONED BY (months(event_date))` | Partition by month |
| `years` | `PARTITIONED BY (years(event_date))` | Partition by year |
| `hours` | `PARTITIONED BY (hours(event_time))` | Partition by hour |
| `bucket` | `PARTITIONED BY (bucket(16, user_id))` | Hash bucket |
| `truncate` | `PARTITIONED BY (truncate(10, name))` | Truncate string to prefix |

### Partition Evolution
You can change partitioning without rewriting data:
```sql
-- Change from monthly to daily partitioning
ALTER TABLE silver_operation_log
REPLACE PARTITION FIELD months(start_time) WITH days(start_time);
```
Old files keep their old partition layout; new files use the new layout. Queries work seamlessly.

---

## 7. Row-Level Operations: MERGE, UPDATE, DELETE

Iceberg v2 supports row-level operations that are critical for the SCD2 pattern.

### MERGE INTO
```sql
MERGE INTO silver_operator t
USING changed_pks s
ON t.operator_id = s.operator_id AND t.is_current = true
WHEN MATCHED THEN UPDATE SET
    t.is_current   = false,
    t.effective_to = current_timestamp(),
    t._updated_at  = current_timestamp()
```

Under the hood, Iceberg implements this as:
1. Find all data files that contain matching rows (using statistics + partition pruning)
2. Read only those files
3. Write **delete files** (position or equality deletes) marking the old rows as deleted
4. Write new data files with the updated rows
5. Atomic commit: new snapshot points to both the original files + delete files + new data files

### How Iceberg Stores Deletes (Format v2)

**Position Deletes:** A delete file that lists `(file_path, row_position)` for each deleted row.
Fast for single-row deletes from large files.

**Equality Deletes:** A delete file that lists column values (like a `WHERE` clause).
Used by Spark when doing equality-based merges.

Readers merge the delete files with data files at query time. Periodically, **compaction**
(table maintenance) rewrites the files to apply deletes and remove the overhead.

---

## 8. Iceberg in AWS: Glue + Athena Integration

### AWS Glue 5.0 + Iceberg

AWS Glue 5.0 ships with Iceberg libraries pre-installed. Enable them with:

```python
# In the Glue job's default arguments:
"--datalake-formats": "iceberg"
```

This automatically adds `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
to the Spark session — which enables `MERGE INTO`, `UPDATE`, `DELETE`, and Iceberg DDL.

The catalog is configured via `SparkConf` **before** `SparkContext` is created:

```python
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

conf = SparkConf()
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET_NAME}/")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
```

> **Why before SparkContext?** `spark.sql.extensions` is a **static configuration** — it
> cannot be changed after the Spark session starts. Glue sets it as part of the session
> initialization, so `spark.conf.set("spark.sql.extensions", ...)` fails with
> `Cannot modify the value of a static config`. Passing via `SparkConf()` before the
> context starts works because the config is applied during initialization.

### Amazon Athena + Iceberg

Athena v3 supports Iceberg natively. Tables created via Glue appear in Athena automatically:

```sql
-- Athena: query an Iceberg table
SELECT * FROM mfg_silver_db.silver_operator WHERE is_current = true;

-- Time travel
SELECT * FROM mfg_silver_db.silver_operator
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 12:00:00';

-- Inspect table snapshots
SELECT * FROM mfg_silver_db."silver_operator$snapshots";
```

### AWS Lake Formation + Iceberg

Lake Formation provides column-level and row-level access control on Iceberg tables.
Permissions work exactly the same as for Hive-format tables:
- Grant `SELECT` on specific tables or columns
- Grant `INSERT`, `DELETE` to ETL roles
- Deny access to PII columns for non-privileged users

---

## 9. Iceberg Table Properties Used in This Project

Every table in this project is created with:

```sql
CREATE TABLE IF NOT EXISTS glue_catalog.mfg_silver_db.silver_operator (
    ...
) USING iceberg
PARTITIONED BY (is_current)
LOCATION 's3://mfg-lakehouse-ACCOUNT-REGION/silver/operator/'
TBLPROPERTIES (
    'table_type'     = 'ICEBERG',
    'format-version' = '2'
)
```

| Property | Value | Reason |
|----------|-------|--------|
| `table_type` | `ICEBERG` | Required by Glue catalog to identify table as Iceberg |
| `format-version` | `2` | Enables row-level deletes for MERGE INTO |

The `LOCATION` tells Iceberg exactly where to write data files in S3.
Without this, Iceberg uses the catalog's warehouse root and auto-generates a path.

---

## 10. Iceberg Catalog: What It Is and Why It Matters

A **catalog** is the registry that maps table names to their current metadata location.

```
Table name:  glue_catalog.mfg_silver_db.silver_operator
                 ↓catalog     ↓database     ↓table
Catalog lookup: "where is the latest metadata.json for silver_operator?"
Catalog returns: s3://bucket/silver/operator/metadata/v5.metadata.json
```

### AWS Glue Data Catalog as Iceberg Catalog

The AWS Glue Data Catalog serves as the Iceberg catalog in this project. Every time an
Iceberg job commits a new snapshot, it updates the Glue table's metadata location parameter
to point to the new `metadata.json` file.

This means:
- Any Glue job, Athena query, or other Iceberg-compatible tool can find the table by name
- Lake Formation enforces access control on top of the catalog
- No separate catalog service is needed (unlike Apache Hive Metastore for on-prem)

```
Spark job writes to silver_operator:
  1. Writes Parquet data files to S3
  2. Writes manifest files to S3
  3. Writes new metadata.json to S3
  4. Calls Glue API: UpdateTable(metadata_location = "s3://.../metadata/v2.metadata.json")
  5. Future reads use v2.metadata.json as the table definition
```

See [06_lake_formation.md](06_lake_formation.md) for how Lake Formation governs access to
these catalog entries and the underlying S3 data.
