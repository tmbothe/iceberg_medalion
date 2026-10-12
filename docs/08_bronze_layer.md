# Bronze Layer — Raw Ingestion

## Table of Contents
1. [Bronze Layer Design Principles](#1-bronze-layer-design-principles)
2. [Source Files and Formats](#2-source-files-and-formats)
3. [Bronze Schema: All Entities](#3-bronze-schema-all-entities)
4. [Job 01: Raw → Bronze Operator (CSV)](#4-job-01-raw--bronze-operator-csv)
5. [Job 02: Raw → Bronze Machine (CSV)](#5-job-02-raw--bronze-machine-csv)
6. [Job 03: Raw → Bronze Material (CSV)](#6-job-03-raw--bronze-material-csv)
7. [Job 04: Raw → Bronze Supplier (JSON + Pivot)](#7-job-04-raw--bronze-supplier-json--pivot)
8. [Job 05: Raw → Bronze Operation Log (JSON + Flatten)](#8-job-05-raw--bronze-operation-log-json--flatten)
9. [Audit Columns](#9-audit-columns)
10. [Idempotency and Re-runs](#10-idempotency-and-re-runs)
11. [Running Bronze Jobs Manually](#11-running-bronze-jobs-manually)

---

## 1. Bronze Layer Design Principles

### "Land as-is" Contract

The Bronze layer's single responsibility is to capture data exactly as it arrived from
the source, with zero data loss and minimal transformation. The only operations allowed are:
- Read the source file
- Add audit metadata columns (`_ingest_timestamp`, `_source_file`, `_load_batch`)
- Write to an Iceberg table (append mode)

No type casting. No filtering. No business logic. No deduplication.

**Why all strings?**
If a value in a CSV is `"15.a"` where we expected a number, Bronze stores `"15.a"` faithfully.
Silver will deal with the casting — and if Silver's logic is wrong, we can fix it and
replay from Bronze without losing the original data.

### Append-Only

Bronze tables are never updated or deleted. Every run of a bronze job appends new rows.
The `_load_batch` partition column enables filtering by batch without scanning all data.

### Iceberg for Bronze

Even though Bronze is just an append store, using Iceberg gives us:
- Schema enforcement (columns must match the defined schema)
- ACID appends — no partial writes
- Time travel — can query "what did the bronze table look like after data_1 was loaded?"
- Glue catalog registration — discoverable by any Spark/Athena engine

---

## 2. Source Files and Formats

| Entity | Format | File Path | Notes |
|--------|--------|-----------|-------|
| Operator | CSV with header | `raw/{batch}/operator/operator.csv` | Straightforward flat CSV |
| Machine | CSV with header | `raw/{batch}/machine/machine.csv` | Straightforward flat CSV |
| Material | CSV with header | `raw/{batch}/material/material.csv` | Straightforward flat CSV |
| Supplier | JSON array | `raw/{batch}/supplier/supplier.json` | Nested `supplier_details` key-value array |
| Operation Log | JSON array | `raw/{batch}/operation_log/operation_log.json` | Embedded `work_order_details` object |

### data_1 vs data_2

| Batch | Records per entity | Purpose |
|-------|--------------------|---------|
| `data_1` | 8 operators, 8 machines, 8 materials, 8 suppliers, 10 ops | Initial load |
| `data_2` | 10 operators, 9 machines, 10 materials, 9 suppliers, 5 ops | Incremental (SCD2 changes + new + unchanged) |

---

## 3. Bronze Schema: All Entities

### bronze_operator
```
operator_id        STRING
operator_name      STRING
workstation_id     STRING
skill_level        STRING
hire_date          STRING
shift              STRING
status             STRING
_ingest_timestamp  TIMESTAMP
_source_file       STRING
_load_batch        STRING
```
Partition: `_load_batch`

### bronze_machine
```
machine_id             STRING
machine_name           STRING
machine_type           STRING
supplier_id            STRING
status                 STRING
purchase_date          STRING
capacity_per_hour      STRING
last_maintenance_date  STRING
_ingest_timestamp      TIMESTAMP
_source_file           STRING
_load_batch            STRING
```
Partition: `_load_batch`

### bronze_material
```
material_id       STRING
material_name     STRING
material_type     STRING
supplier_id       STRING
unit_cost         STRING
unit_of_measure   STRING
stock_quantity    STRING
reorder_level     STRING
_ingest_timestamp TIMESTAMP
_source_file      STRING
_load_batch       STRING
```
Partition: `_load_batch`

### bronze_supplier
```
supplier_id         STRING
supplier_machine_id STRING
company_name        STRING
country             STRING
contact_email       STRING
lead_time_days      STRING
rating              STRING
_ingest_timestamp   TIMESTAMP
_source_file        STRING
_load_batch         STRING
```
Partition: `_load_batch`
> Note: `supplier_details` key-value array is pivoted to flat columns in Bronze

### bronze_operation_log
```
oplog_id           STRING
work_order_id      STRING
machine_id         STRING
operator_id        STRING
workstation_id     STRING
material_id        STRING
operation_type     STRING
start_time         STRING
end_time           STRING
status             STRING
quantity_processed STRING
defects_found      STRING
quality_score      STRING
notes              STRING
defect_type        STRING
_ingest_timestamp  TIMESTAMP
_source_file       STRING
_load_batch        STRING
```
Partition: `_load_batch`
> Note: `work_order_details` embedded object is flattened in Bronze

---

## 4. Job 01: Raw → Bronze Operator (CSV)

**Script:** `glue_jobs/01_raw_to_bronze_operator.py`
**Glue Job:** `mfg-raw-to-bronze-operator`

### What it does:
1. Reads `raw/{LOAD_BATCH}/operator/operator.csv` with `inferSchema=false` (all strings)
2. Adds three audit columns
3. Creates `bronze_operator` Iceberg table if it doesn't exist
4. Appends the rows

### Key code:
```python
SOURCE_PATH  = f"s3://{BUCKET_NAME}/raw/{LOAD_BATCH}/operator/operator.csv"
TARGET_TABLE = "glue_catalog.mfg_bronze_db.bronze_operator"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \   ← all columns read as strings
    .csv(SOURCE_PATH)

df = df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit(SOURCE_PATH)) \
    .withColumn("_load_batch", lit(LOAD_BATCH))

df.writeTo(TARGET_TABLE).append()
```

### Table properties:
```sql
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
```

---

## 5. Job 02: Raw → Bronze Machine (CSV)

**Script:** `glue_jobs/02_raw_to_bronze_machine.py`
**Glue Job:** `mfg-raw-to-bronze-machine`

Identical pattern to operator but reads `machine/machine.csv` and writes to
`bronze_machine`. The machine CSV has two additional columns:
`capacity_per_hour` and `last_maintenance_date`.

---

## 6. Job 03: Raw → Bronze Material (CSV)

**Script:** `glue_jobs/03_raw_to_bronze_material.py`
**Glue Job:** `mfg-raw-to-bronze-material`

Same CSV pattern. Material has numeric fields (`unit_cost`, `stock_quantity`, `reorder_level`)
that are stored as strings in Bronze — type casting happens in Silver.

---

## 7. Job 04: Raw → Bronze Supplier (JSON + Pivot)

**Script:** `glue_jobs/04_raw_to_bronze_supplier.py`
**Glue Job:** `mfg-raw-to-bronze-supplier`

This job is more complex because the source JSON has a nested key-value array:

### Source JSON Structure
```json
[
  {
    "supplier_id": "SUP001",
    "supplier_machine_id": "M001",
    "supplier_details": [
      {"detail_key": "company_name",   "detail_value": "TechSupply Co"},
      {"detail_key": "country",        "detail_value": "USA"},
      {"detail_key": "contact_email",  "detail_value": "contact@techsupply.com"},
      {"detail_key": "lead_time_days", "detail_value": "7"},
      {"detail_key": "rating",         "detail_value": "4.5"}
    ]
  },
  ...
]
```

### Transformation: Explode → Pivot

```python
# Step 1: Read JSON, infer schema (Spark handles the nested array)
df_raw = spark.read.option("multiline", "true").json(SOURCE_PATH)

# Step 2: Explode the supplier_details array
# Each row becomes one row per detail_key
df_exploded = df_raw.select(
    col("supplier_id"),
    col("supplier_machine_id"),
    explode(col("supplier_details")).alias("detail")
).select(
    col("supplier_id"),
    col("supplier_machine_id"),
    col("detail.detail_key").alias("detail_key"),
    col("detail.detail_value").alias("detail_value")
)
# Result: one row per supplier × detail_key combination

# Step 3: Pivot — turn rows into columns
df_pivot = df_exploded \
    .groupBy("supplier_id", "supplier_machine_id") \
    .pivot("detail_key", ["company_name", "country", "contact_email",
                           "lead_time_days", "rating"]) \
    .agg(first("detail_value"))
# Result: one row per supplier, with company_name/country/etc. as columns
```

### Before Pivot:
```
supplier_id | supplier_machine_id | detail_key   | detail_value
SUP001      | M001                | company_name | TechSupply Co
SUP001      | M001                | country      | USA
SUP001      | M001                | contact_email| contact@techsupply.com
SUP001      | M001                | lead_time_days| 7
SUP001      | M001                | rating       | 4.5
```

### After Pivot:
```
supplier_id | supplier_machine_id | company_name  | country | contact_email          | lead_time_days | rating
SUP001      | M001                | TechSupply Co | USA     | contact@techsupply.com | 7              | 4.5
```

---

## 8. Job 05: Raw → Bronze Operation Log (JSON + Flatten)

**Script:** `glue_jobs/05_raw_to_bronze_operation_log.py`
**Glue Job:** `mfg-raw-to-bronze-oplog`

The operation log has an embedded `work_order_details` object (not an array):

### Source JSON Structure
```json
{
  "oplog_id": "OL001",
  "work_order_id": "WO-2024-001",
  "machine_id": "M001",
  "work_order_details": {
    "operator_id": "OP001",
    "workstation_id": "WS-A",
    "material_id": "MAT001",
    "operation_type": "Assembly",
    "start_time": "2024-01-15 08:00:00",
    "end_time": "2024-01-15 10:30:00",
    "status": "Completed",
    "quantity_processed": "150",
    "defects_found": "3",
    "quality_score": "98.0",
    "notes": "Routine assembly",
    "defect_type": "Surface Scratch"
  }
}
```

### Transformation: Flatten Nested Object

```python
df_raw = spark.read.option("multiline", "true").json(SOURCE_PATH)

# Flatten by selecting nested fields with dot notation
df_flat = df_raw.select(
    col("oplog_id").cast(StringType()),
    col("work_order_id").cast(StringType()),
    col("machine_id").cast(StringType()),
    col("work_order_details.operator_id").alias("operator_id").cast(StringType()),
    col("work_order_details.workstation_id").alias("workstation_id").cast(StringType()),
    col("work_order_details.material_id").alias("material_id").cast(StringType()),
    col("work_order_details.operation_type").alias("operation_type").cast(StringType()),
    col("work_order_details.start_time").alias("start_time").cast(StringType()),
    col("work_order_details.end_time").alias("end_time").cast(StringType()),
    col("work_order_details.status").alias("status").cast(StringType()),
    col("work_order_details.quantity_processed").alias("quantity_processed").cast(StringType()),
    col("work_order_details.defects_found").alias("defects_found").cast(StringType()),
    col("work_order_details.quality_score").alias("quality_score").cast(StringType()),
    col("work_order_details.notes").alias("notes").cast(StringType()),
    col("work_order_details.defect_type").alias("defect_type").cast(StringType())
)
```

All values are cast to `StringType()` — this is the Bronze contract.

---

## 9. Audit Columns

Every Bronze table row contains three audit columns:

| Column | Type | Content | Example |
|--------|------|---------|---------|
| `_ingest_timestamp` | TIMESTAMP | When the row was written to Bronze | `2024-01-15 10:30:00` |
| `_source_file` | STRING | Full S3 URI of the source file | `s3://bucket/raw/data_1/operator/operator.csv` |
| `_load_batch` | STRING | The batch name | `data_1` |

These columns enable full lineage tracing:
- "When did this record arrive?"
- "Which file did it come from?"
- "Which batch was it part of?"

The `_load_batch` column also serves as the **partition column** — queries that filter
on `_load_batch` read only one partition's worth of files, making batch-specific
queries very efficient.

---

## 10. Idempotency and Re-runs

Bronze jobs are designed to be safely re-runnable. If you run a bronze job twice with
the same `LOAD_BATCH`, you will get duplicate rows in Bronze — there is no dedup at this layer.

**This is intentional.** Bronze is a raw landing zone. If you accidentally double-load,
the Silver job's `dropDuplicates([pk_col])` call will handle duplicates when reading
from Bronze.

To cleanly re-run a batch:
1. Run `00_cleanup_tables.py` to drop all tables (it drops Bronze too)
2. Re-run the bronze jobs for the affected batch
3. Re-run Silver and Gold

---

## 11. Running Bronze Jobs Manually

If you need to run a specific bronze job manually (without Step Functions):

```bash
# Run the operator bronze job for data_1
aws glue start-job-run \
    --job-name mfg-raw-to-bronze-operator \
    --arguments '{
        "--LOAD_BATCH": "data_1",
        "--BUCKET_NAME": "mfg-lakehouse-442042510197-us-east-1"
    }'

# Monitor job status
JOB_RUN_ID=$(aws glue start-job-run \
    --job-name mfg-raw-to-bronze-operator \
    --arguments '{"--LOAD_BATCH":"data_2","--BUCKET_NAME":"mfg-lakehouse-442042510197-us-east-1"}' \
    --query 'JobRunId' --output text)

aws glue get-job-run --job-name mfg-raw-to-bronze-operator --run-id $JOB_RUN_ID \
    --query 'JobRun.{State:JobRunState,Message:ErrorMessage}'
```

### Verify Bronze Load in Athena

```sql
-- Count rows per batch
SELECT _load_batch, COUNT(*) as row_count
FROM mfg_bronze_db.bronze_operator
GROUP BY _load_batch;
-- Expected: data_1 → 8 rows, data_2 → 10 rows

-- Inspect a bronze row
SELECT * FROM mfg_bronze_db.bronze_operator
WHERE _load_batch = 'data_1' LIMIT 5;
```

Continue to [09_silver_layer.md](09_silver_layer.md) for the Silver transformation details.
