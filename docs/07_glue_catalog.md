# AWS Glue Data Catalog

## Table of Contents
1. [What is the Glue Data Catalog?](#1-what-is-the-glue-data-catalog)
2. [Catalog Structure in This Project](#2-catalog-structure-in-this-project)
3. [Databases](#3-databases)
4. [How Iceberg Tables Are Registered](#4-how-iceberg-tables-are-registered)
5. [Glue ETL Jobs](#5-glue-etl-jobs)
6. [Glue Job Configuration: Key Parameters](#6-glue-job-configuration-key-parameters)
7. [Viewing and Managing the Catalog](#7-viewing-and-managing-the-catalog)

---

## 1. What is the Glue Data Catalog?

The AWS Glue Data Catalog is a centralized metadata repository — a managed Apache Hive
Metastore-compatible service. It stores:

- **Database definitions** — logical namespaces grouping related tables
- **Table definitions** — schema (column names and types), partition info, storage location
- **Table properties** — for Iceberg tables, the `metadata_location` pointing to the
  current `metadata.json` file in S3

Every Spark/Athena/Glue query that uses `glue_catalog.<database>.<table>` looks up the
table definition in the Glue Data Catalog to find the S3 location and schema.

```
Query: SELECT * FROM glue_catalog.mfg_silver_db.silver_operator
  ↓
Glue Catalog: "silver_operator is at s3://bucket/silver/operator/metadata/v3.metadata.json"
  ↓
Iceberg library reads metadata.json → finds manifest list → finds data files
  ↓
Returns rows
```

### Glue Catalog vs. Hive Metastore

| Feature | Glue Catalog | Hive Metastore |
|---------|-------------|----------------|
| Hosting | Managed by AWS | Self-hosted (EMR, on-prem) |
| Cost | $1/100K API calls | EC2/EMR cost |
| Integration | Native with Glue, Athena, Lake Formation | Requires connector |
| HA | Automatic | Requires setup |
| Cross-account | Yes (resource links) | No |

---

## 2. Catalog Structure in This Project

```
Glue Data Catalog
└── Databases
    ├── mfg_bronze_db
    │   ├── bronze_operator
    │   ├── bronze_machine
    │   ├── bronze_material
    │   ├── bronze_supplier
    │   └── bronze_operation_log
    ├── mfg_silver_db
    │   ├── silver_operator
    │   ├── silver_machine
    │   ├── silver_material
    │   ├── silver_supplier
    │   └── silver_operation_log
    └── mfg_gold_db
        ├── gold_operation_summary
        ├── gold_defect_analysis
        └── gold_supplier_performance
```

All 13 tables are **Iceberg tables** — the Glue catalog entry contains a special property
`metadata_location` that points to the current Iceberg metadata file.

---

## 3. Databases

Three Glue databases correspond to the three medallion layers:

| Database | Layer | Description |
|----------|-------|-------------|
| `mfg_bronze_db` | Bronze | Raw ingested data, all-string, partitioned by `_load_batch` |
| `mfg_silver_db` | Silver | Cleansed + typed data with SCD2 history |
| `mfg_gold_db` | Gold | Aggregated business reports |

### Create Databases

```bash
aws glue create-database \
    --database-input '{
        "Name": "mfg_bronze_db",
        "Description": "Manufacturing Lakehouse — Bronze layer (raw ingestion)",
        "Parameters": {"layer": "bronze", "project": "mfg-lakehouse"}
    }'
```

### Verify Databases

```bash
aws glue get-databases --query 'DatabaseList[?contains(Name,`mfg`)].{Name:Name,Description:Description}'
```

---

## 4. How Iceberg Tables Are Registered

When a Glue ETL job runs `CREATE TABLE IF NOT EXISTS ... USING iceberg`, Iceberg:

1. Creates the initial `v1.metadata.json` in S3
2. Registers the table in Glue with these properties:
   ```
   Parameters:
     table_type = "ICEBERG"
     metadata_location = "s3://bucket/silver/operator/metadata/v1.metadata.json"
   ```
3. Sets the `StorageDescriptor.Location` to the Iceberg data folder

On every subsequent write (append, merge, overwrite), the `metadata_location` parameter
is updated atomically to point to the new version.

### Inspecting Table Properties

```bash
aws glue get-table \
    --database-name mfg_silver_db \
    --name silver_operator \
    --query 'Table.Parameters'
# {
#   "table_type": "ICEBERG",
#   "metadata_location": "s3://mfg-lakehouse-.../silver/operator/metadata/v5.metadata.json",
#   "format-version": "2",
#   ...
# }
```

### Why `CREATE TABLE IF NOT EXISTS` Can Be a Problem

If the table was previously created pointing to a different S3 location (e.g., a different
account ID in the bucket name), `CREATE TABLE IF NOT EXISTS` will skip creation — leaving
the old, incorrect location in place. All subsequent writes go to the wrong bucket.

**Always drop and recreate tables if the bucket changes.** Use `00_cleanup_tables.py`
(run as a Glue job) to drop all tables, then re-run the pipeline from scratch.

---

## 5. Glue ETL Jobs

Each Glue job is a PySpark script that runs on managed Spark infrastructure.

### Jobs in This Project

| Job Name | Script | Purpose |
|----------|--------|---------|
| `mfg-raw-to-bronze-operator` | `01_raw_to_bronze_operator.py` | CSV → bronze_operator |
| `mfg-raw-to-bronze-machine` | `02_raw_to_bronze_machine.py` | CSV → bronze_machine |
| `mfg-raw-to-bronze-material` | `03_raw_to_bronze_material.py` | CSV → bronze_material |
| `mfg-raw-to-bronze-supplier` | `04_raw_to_bronze_supplier.py` | JSON → bronze_supplier |
| `mfg-raw-to-bronze-oplog` | `05_raw_to_bronze_operation_log.py` | JSON → bronze_operation_log |
| `mfg-bronze-to-silver` | `06_bronze_to_silver.py` | Bronze → Silver (SCD2 + append) |
| `mfg-silver-to-gold` | `07_silver_to_gold.py` | Silver → Gold (aggregations) |
| `mfg-cleanup-tables` | `00_cleanup_tables.py` | Drop all Iceberg tables (maintenance) |

### Job Resource Configuration

All jobs use:
```
Worker type:    G.1X  (1 DPU = 4 vCPU, 16 GB RAM)
Workers:        2
Glue version:   5.0
Timeout:        30 minutes
Max retries:    1
```

**G.1X** is sufficient for the small dataset in this project. For larger datasets:
- Consider G.2X (8 vCPU, 32 GB RAM)
- Increase number of workers
- Enable auto-scaling: `--enable-auto-scaling true`

---

## 6. Glue Job Configuration: Key Parameters

### Default Arguments

Every Glue job is created with these default arguments:

```json
{
    "--job-language":            "python",
    "--job-bookmark-option":     "job-bookmark-disable",
    "--datalake-formats":        "iceberg",
    "--TempDir":                 "s3://bucket/glue-temp/",
    "--enable-metrics":          "true",
    "--enable-spark-ui":         "true",
    "--spark-event-logs-path":   "s3://bucket/spark-ui/",
    "--enable-glue-datacatalog": "true",
    "--BUCKET_NAME":             "mfg-lakehouse-...",
    "--LOAD_BATCH":              "data_1"
}
```

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `--datalake-formats` | `iceberg` | Pre-loads Iceberg libraries and sets `spark.sql.extensions` |
| `--job-bookmark-option` | `job-bookmark-disable` | We manage idempotency ourselves (batch filter) |
| `--enable-glue-datacatalog` | `true` | Use Glue catalog for Hive-style table lookups |
| `--BUCKET_NAME` | `mfg-lakehouse-...` | Passed to script for S3 path construction |
| `--LOAD_BATCH` | `data_1` | Which batch folder to read from raw/ |

### Why `--datalake-formats iceberg` Instead of Manual Config

Glue 5.0 pre-bundles all Iceberg JARs. The `--datalake-formats iceberg` parameter:
1. Adds Iceberg JARs to the Spark classpath
2. Sets `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`
   as a **static config** at Spark startup

Because it's set as a static config at startup, you **cannot** set it again via
`spark.conf.set()` — that will throw `Cannot modify the value of a static config`.
The scripts avoid this by not calling `spark.conf.set("spark.sql.extensions", ...)`.

The catalog configs (`spark.sql.catalog.glue_catalog.*`) are **not** static and must be
set in the script via `SparkConf()` before `SparkContext` creation:

```python
conf = SparkConf()
conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET_NAME}/")
conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
```

---

## 7. Viewing and Managing the Catalog

### AWS Console

1. Go to **AWS Glue → Data Catalog → Databases**
2. Click a database → **Tables** to see all registered tables
3. Click a table → **Schema** to see columns
4. Click **Parameters** to see `metadata_location` (for Iceberg tables)

### AWS CLI

```bash
# List all tables in a database
aws glue get-tables \
    --database-name mfg_silver_db \
    --query 'TableList[].{Name:Name,Location:StorageDescriptor.Location,Format:Parameters.table_type}'

# Get a specific table's full definition
aws glue get-table \
    --database-name mfg_silver_db \
    --name silver_operator

# List all columns (schema)
aws glue get-table \
    --database-name mfg_silver_db \
    --name silver_operator \
    --query 'Table.StorageDescriptor.Columns[].{Name:Name,Type:Type}'
```

### Athena (SQL)

```sql
-- List all databases
SHOW DATABASES;

-- List tables in a database
SHOW TABLES IN mfg_silver_db;

-- Describe a table
DESCRIBE mfg_silver_db.silver_operator;

-- Show table properties (Iceberg metadata)
SHOW TBLPROPERTIES mfg_silver_db.silver_operator;

-- Inspect Iceberg snapshots
SELECT * FROM mfg_silver_db."silver_operator$snapshots";

-- Inspect Iceberg files
SELECT * FROM mfg_silver_db."silver_operator$files";
```

### Verify Table Count After Pipeline Run

```bash
# Count tables in each database (expect 5, 5, 3)
for DB in mfg_bronze_db mfg_silver_db mfg_gold_db; do
    COUNT=$(aws glue get-tables --database-name ${DB} --query 'length(TableList)')
    echo "${DB}: ${COUNT} tables"
done
```
