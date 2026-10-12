# Silver Layer — Cleansing, SCD2 & Transformation

## Table of Contents
1. [Silver Layer Purpose](#1-silver-layer-purpose)
2. [Silver Schema: All Entities](#2-silver-schema-all-entities)
3. [The run_scd2() Helper Function](#3-the-run_scd2-helper-function)
4. [SCD2 Dimensions: Operator, Machine, Material, Supplier](#4-scd2-dimensions-operator-machine-material-supplier)
5. [Fact Table: Operation Log (Append-Only + Typed)](#5-fact-table-operation-log-append-only--typed)
6. [Derived Columns in Silver Operation Log](#6-derived-columns-in-silver-operation-log)
7. [Initial vs Incremental Load](#7-initial-vs-incremental-load)
8. [Running the Silver Job](#8-running-the-silver-job)
9. [Validating Silver Results](#9-validating-silver-results)

---

## 1. Silver Layer Purpose

Silver is where raw Bronze data becomes **trusted, typed, and historically tracked**.

The Silver job (`06_bronze_to_silver.py`) performs:

| Task | How |
|------|-----|
| Type casting | String → INTEGER, DECIMAL, TIMESTAMP |
| SCD Type 2 for dimensions | MERGE INTO to expire old versions, append new versions |
| Composite key creation | Supplier: `supplier_id~supplier_machine_id` synthetic PK |
| Derived columns | `duration_minutes`, `defect_rate` computed from raw values |
| Deduplication | `dropDuplicates([pk_col])` to guard against double-ingested Bronze rows |

**What Silver does NOT do:**
- Join dimensions to facts (that's Gold's job)
- Aggregate (that's Gold's job)
- Modify Bronze (Bronze is immutable)

---

## 2. Silver Schema: All Entities

### silver_operator (SCD2)
```
operator_id     STRING       ← business primary key
operator_name   STRING
workstation_id  STRING
skill_level     STRING
hire_date       STRING       ← not tracked (doesn't change)
shift           STRING       ← tracked (SCD2 change triggers)
status          STRING       ← tracked
effective_from  TIMESTAMP    ← when this version became current
effective_to    TIMESTAMP    ← when this version was superseded (NULL = current)
is_current      BOOLEAN      ← TRUE = current version
record_hash     STRING       ← MD5 of tracked columns
_created_at     TIMESTAMP
_updated_at     TIMESTAMP

Partition: is_current
```

### silver_machine (SCD2)
```
machine_id             STRING
machine_name           STRING
machine_type           STRING
supplier_id            STRING
status                 STRING       ← tracked
purchase_date          STRING       ← not tracked
capacity_per_hour      STRING       ← tracked
last_maintenance_date  STRING       ← tracked
effective_from  TIMESTAMP
effective_to    TIMESTAMP
is_current      BOOLEAN
record_hash     STRING
_created_at     TIMESTAMP
_updated_at     TIMESTAMP
```

### silver_material (SCD2)
```
material_id       STRING
material_name     STRING
material_type     STRING
supplier_id       STRING       ← tracked
unit_cost         STRING       ← tracked
unit_of_measure   STRING       ← tracked
stock_quantity    STRING       ← tracked
reorder_level     STRING       ← not tracked in record_hash
effective_from    TIMESTAMP
effective_to      TIMESTAMP
is_current        BOOLEAN
record_hash       STRING
_created_at       TIMESTAMP
_updated_at       TIMESTAMP
```

### silver_supplier (SCD2)
```
supplier_pk         STRING   ← synthetic: "SUP001~M001"
supplier_id         STRING
supplier_machine_id STRING
company_name        STRING   ← tracked
country             STRING   ← tracked
contact_email       STRING   ← tracked
lead_time_days      STRING   ← tracked
rating              STRING   ← tracked
effective_from      TIMESTAMP
effective_to        TIMESTAMP
is_current          BOOLEAN
record_hash         STRING
_created_at         TIMESTAMP
_updated_at         TIMESTAMP
```

### silver_operation_log (Append-only)
```
oplog_id            STRING
work_order_id       STRING
machine_id          STRING
workstation_id      STRING
material_id         STRING
operator_id         STRING
operation_type      STRING
start_time          TIMESTAMP     ← cast from string
end_time            TIMESTAMP     ← cast from string
duration_minutes    DOUBLE        ← derived: (end_time - start_time) / 60
status              STRING
quantity_processed  INTEGER       ← cast from string
defects_found       INTEGER       ← cast from string
defect_rate         DECIMAL(5,2)  ← derived: defects / qty * 100
defect_type         STRING
quality_score       DECIMAL(5,2)  ← cast from string
notes               STRING
_ingest_timestamp   TIMESTAMP

Partition: days(start_time)
```

---

## 3. The `run_scd2()` Helper Function

All four dimension tables share the same SCD2 logic, encapsulated in a reusable function:

```python
def run_scd2(bronze_table, silver_table, silver_location, pk_col, track_cols,
             create_ddl, incoming_df):
```

**Parameters:**

| Parameter | Example | Description |
|-----------|---------|-------------|
| `bronze_table` | `"mfg_bronze_db.bronze_operator"` | Source table name (for logging) |
| `silver_table` | `"glue_catalog.mfg_silver_db.silver_operator"` | Target Iceberg table |
| `silver_location` | `"s3://.../silver/operator/"` | S3 location for CREATE TABLE |
| `pk_col` | `"operator_id"` | Primary key column name |
| `track_cols` | `["operator_name", "shift", "status", ...]` | Columns to hash for change detection |
| `create_ddl` | `"CREATE TABLE IF NOT EXISTS ..."` | DDL to create table if not exists |
| `incoming_df` | DataFrame | Filtered Bronze rows for this batch |

**What the function does:**

```
1. Run create_ddl (CREATE TABLE IF NOT EXISTS — no-op if table exists)
2. Deduplicate incoming_df by pk_col
3. Compute record_hash = MD5(concat(track_cols))
4. Branch on LOAD_TYPE:
   a. initial: write all rows as current → createOrReplace()
   b. incremental:
      - join incoming with existing current rows (is_current=true)
      - categorize: NEW, CHANGED, UNCHANGED
      - MERGE INTO silver to expire CHANGED rows
      - writeTo(silver).append() for NEW + CHANGED rows
```

---

## 4. SCD2 Dimensions: Operator, Machine, Material, Supplier

### Operator

```python
operator_bronze = spark.table("glue_catalog.mfg_bronze_db.bronze_operator") \
    .filter(f"_load_batch = '{LOAD_BATCH}'") \
    .select("operator_id", "operator_name", "workstation_id",
            "skill_level", "hire_date", "shift", "status")

run_scd2(
    pk_col     = "operator_id",
    track_cols = ["operator_name", "workstation_id", "skill_level", "shift", "status"],
    ...
)
```

**Tracked columns:** `operator_name`, `workstation_id`, `skill_level`, `shift`, `status`
→ Any change in any of these triggers a new SCD2 version.
**Not tracked:** `hire_date` — immutable after hire.

### Supplier (Composite Key)

The supplier entity is unique because one supplier can supply multiple machines.
The natural key is the combination `(supplier_id, supplier_machine_id)`:

```python
supplier_bronze = spark.table("glue_catalog.mfg_bronze_db.bronze_supplier") \
    .filter(f"_load_batch = '{LOAD_BATCH}'") \
    .withColumn("supplier_pk",
        concat_ws("~", col("supplier_id"), col("supplier_machine_id"))
    ) \
    .select("supplier_pk", "supplier_id", "supplier_machine_id",
            "company_name", "country", "contact_email",
            "lead_time_days", "rating")

run_scd2(
    pk_col     = "supplier_pk",   # "SUP001~M001", "SUP001~M002", etc.
    track_cols = ["company_name", "country", "contact_email", "lead_time_days", "rating"],
    ...
)
```

The synthetic key `supplier_pk` is stored in the Silver table. Downstream joins
can use either `supplier_pk` (exact match) or `supplier_id` (to get all machines
from a supplier).

---

## 5. Fact Table: Operation Log (Append-Only + Typed)

The operation log is a **fact table** — it records immutable events (operations performed).
It does not use SCD2; new events are simply appended.

However, Silver does perform type casting that Bronze skipped:

```python
bronze_log = spark.table("glue_catalog.mfg_bronze_db.bronze_operation_log") \
    .filter(f"_load_batch = '{LOAD_BATCH}'")

silver_log = bronze_log \
    .withColumn("start_time_ts", to_timestamp(col("start_time"))) \
    .withColumn("end_time_ts",   to_timestamp(col("end_time"))) \
    .withColumn("qty",           col("quantity_processed").cast(IntegerType())) \
    .withColumn("defects",       col("defects_found").cast(IntegerType())) \
    .withColumn("q_score",       col("quality_score").cast(DecimalType(5, 2))) \
    .withColumn("duration_minutes",
        (unix_timestamp(col("end_time_ts")) - unix_timestamp(col("start_time_ts"))) / 60.0
    ) \
    .withColumn("defect_rate",
        when(col("qty") > 0,
             round(col("defects").cast("double") / col("qty") * 100, 2)
        ).otherwise(lit(0.0))
    ) \
    .select(
        col("oplog_id"),
        col("start_time_ts").alias("start_time"),
        col("end_time_ts").alias("end_time"),
        col("duration_minutes"),
        col("qty").alias("quantity_processed"),
        col("defects").alias("defects_found"),
        col("defect_rate"),
        col("q_score").alias("quality_score"),
        # ... other columns
        NOW.alias("_ingest_timestamp")
    )

silver_log.writeTo("glue_catalog.mfg_silver_db.silver_operation_log").append()
```

---

## 6. Derived Columns in Silver Operation Log

| Column | Formula | Example |
|--------|---------|---------|
| `duration_minutes` | `(unix_timestamp(end_time) - unix_timestamp(start_time)) / 60` | `150.0` (2.5 hours) |
| `defect_rate` | `(defects_found / quantity_processed) * 100` if qty > 0, else 0.0 | `2.0` (2% defect rate) |

**Why compute these in Silver and not Gold?**

- These are **row-level** computations (not aggregations) — they belong in Silver
- Gold aggregates these pre-computed values, which is simpler and faster
- If the formula is wrong, fix Silver and replay; Gold's aggregations automatically update

---

## 7. Initial vs Incremental Load

### Initial Load (`LOAD_TYPE=initial`, `LOAD_BATCH=data_1`)

```python
if LOAD_TYPE == "initial":
    silver_df = incoming \
        .withColumn("effective_from", NOW) \
        .withColumn("effective_to", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("_created_at", NOW) \
        .withColumn("_updated_at", NOW)
    silver_df.writeTo(silver_table).createOrReplace()
```

All incoming rows become the first and current version. `createOrReplace()` ensures a clean
start — safe if the table already has data from a previous failed run.

### Incremental Load (`LOAD_TYPE=incremental`, `LOAD_BATCH=data_2`)

The full SCD2 logic runs (see [04_scd_type2.md](04_scd_type2.md)):

1. Read only current rows from Silver (`is_current=true` partition)
2. Join with incoming Bronze rows
3. Identify NEW / CHANGED / UNCHANGED
4. MERGE to expire CHANGED rows
5. Append new versions of CHANGED + NEW rows

**Expected Silver row counts after both loads:**
```
silver_operator:       13 rows (8 initial + 3 new versions for OP002/003/007 + 2 new OP009/010)
silver_machine:        14 rows (8 initial + 5 new versions for M003/004/007/008 + 1 new M009)
silver_material:       14 rows (8 initial + 4 new versions + 2 new MAT009/010)
silver_supplier:       10 rows (9 initial + 1 new for changed supplier)
silver_operation_log:  15 rows (10 initial + 5 new OL011–OL015, append-only)
```

---

## 8. Running the Silver Job

```bash
# Incremental load (default)
aws glue start-job-run \
    --job-name mfg-bronze-to-silver \
    --arguments '{
        "--LOAD_BATCH": "data_2",
        "--LOAD_TYPE":  "incremental",
        "--BUCKET_NAME":"mfg-lakehouse-442042510197-us-east-1"
    }'

# Initial load
aws glue start-job-run \
    --job-name mfg-bronze-to-silver \
    --arguments '{
        "--LOAD_BATCH": "data_1",
        "--LOAD_TYPE":  "initial",
        "--BUCKET_NAME":"mfg-lakehouse-442042510197-us-east-1"
    }'
```

The job processes all 5 entities sequentially in one Spark session:
1. Operator (SCD2)
2. Machine (SCD2)
3. Material (SCD2)
4. Supplier (SCD2)
5. Operation Log (append + type cast)

---

## 9. Validating Silver Results

### Count rows and current/historical split (Athena)

```sql
-- Check SCD2 correctness for operator
SELECT
    is_current,
    COUNT(*) as row_count
FROM mfg_silver_db.silver_operator
GROUP BY is_current;
-- Expected after initial + incremental:
-- FALSE: 3  (OP002, OP003, OP007 old versions)
-- TRUE:  10 (8 original unchanged + 3 new + 2 brand new)

-- Inspect changed record history for OP002
SELECT
    operator_id, operator_name, workstation_id, shift, status,
    effective_from, effective_to, is_current
FROM mfg_silver_db.silver_operator
WHERE operator_id = 'OP002'
ORDER BY effective_from;

-- Verify operation log derived columns
SELECT
    oplog_id,
    quantity_processed,
    defects_found,
    defect_rate,
    duration_minutes,
    quality_score
FROM mfg_silver_db.silver_operation_log
LIMIT 5;

-- Count by load: Silver picks up from Bronze batch filter
SELECT _ingest_timestamp::DATE as load_date, COUNT(*) as rows
FROM mfg_silver_db.silver_operation_log
GROUP BY 1 ORDER BY 1;
```

### Quick Sanity Check for All Dims

```sql
SELECT 'operator'     as tbl, COUNT(*) total, SUM(CASE WHEN is_current THEN 1 END) current_rows FROM mfg_silver_db.silver_operator
UNION ALL
SELECT 'machine',     COUNT(*), SUM(CASE WHEN is_current THEN 1 END) FROM mfg_silver_db.silver_machine
UNION ALL
SELECT 'material',    COUNT(*), SUM(CASE WHEN is_current THEN 1 END) FROM mfg_silver_db.silver_material
UNION ALL
SELECT 'supplier',    COUNT(*), SUM(CASE WHEN is_current THEN 1 END) FROM mfg_silver_db.silver_supplier;
```
