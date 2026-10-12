# Validation & Testing

## Table of Contents
1. [Validation Strategy](#1-validation-strategy)
2. [Bronze Layer Validation](#2-bronze-layer-validation)
3. [Silver Layer Validation](#3-silver-layer-validation)
4. [Gold Layer Validation](#4-gold-layer-validation)
5. [SCD2 Correctness Tests](#5-scd2-correctness-tests)
6. [Data Lineage Spot Checks](#6-data-lineage-spot-checks)
7. [Iceberg Health Checks](#7-iceberg-health-checks)
8. [Expected Results Reference](#8-expected-results-reference)

---

## 1. Validation Strategy

Validation follows the medallion layer order:
1. **Bronze** — verify raw data landed correctly (counts, schema, no truncation)
2. **Silver** — verify SCD2 logic (correct row counts, correct current/historical split)
3. **Gold** — verify aggregations are mathematically correct

All queries run in **Amazon Athena** against the Glue Data Catalog.

**Configure Athena output location first:**
```sql
-- Or set via AWS Console: Athena → Workgroup → Settings → Query result location
-- s3://mfg-lakehouse-<account>-us-east-1/athena-results/
```

---

## 2. Bronze Layer Validation

### 2a. Row Counts per Batch

```sql
-- Operator
SELECT _load_batch, COUNT(*) as row_count
FROM mfg_bronze_db.bronze_operator
GROUP BY _load_batch ORDER BY _load_batch;
```
| `_load_batch` | `row_count` |
|--------------|-------------|
| data_1 | 8 |
| data_2 | 10 |

```sql
-- Machine
SELECT _load_batch, COUNT(*) FROM mfg_bronze_db.bronze_machine GROUP BY 1;
-- data_1: 8, data_2: 9

-- Material
SELECT _load_batch, COUNT(*) FROM mfg_bronze_db.bronze_material GROUP BY 1;
-- data_1: 8, data_2: 10

-- Supplier
SELECT _load_batch, COUNT(*) FROM mfg_bronze_db.bronze_supplier GROUP BY 1;
-- data_1: 8, data_2: 9

-- Operation Log
SELECT _load_batch, COUNT(*) FROM mfg_bronze_db.bronze_operation_log GROUP BY 1;
-- data_1: 10, data_2: 5
```

### 2b. Verify All Columns Are Populated (No Schema Mismatch)

```sql
-- Check for NULLs in key columns (should all be 0)
SELECT
    SUM(CASE WHEN operator_id IS NULL THEN 1 ELSE 0 END)  as null_ids,
    SUM(CASE WHEN _load_batch IS NULL THEN 1 ELSE 0 END)  as null_batch,
    SUM(CASE WHEN _source_file IS NULL THEN 1 ELSE 0 END) as null_file
FROM mfg_bronze_db.bronze_operator;
```

### 2c. Verify Source File Paths

```sql
SELECT DISTINCT _source_file, _load_batch
FROM mfg_bronze_db.bronze_operator
ORDER BY _load_batch;
-- Should see: s3://.../raw/data_1/operator/operator.csv
--             s3://.../raw/data_2/operator/operator.csv
```

### 2d. Verify Supplier Pivot Was Correct

```sql
-- All supplier detail columns should be populated
SELECT
    supplier_id, supplier_machine_id,
    company_name, country, contact_email,
    lead_time_days, rating
FROM mfg_bronze_db.bronze_supplier
WHERE _load_batch = 'data_1'
ORDER BY supplier_id
LIMIT 5;
-- No NULL values in the pivoted columns
```

### 2e. Verify Operation Log Flattening

```sql
-- work_order_details fields should appear as top-level columns
SELECT
    oplog_id, work_order_id, machine_id,
    operator_id, workstation_id, material_id,
    operation_type, start_time, quantity_processed
FROM mfg_bronze_db.bronze_operation_log
WHERE _load_batch = 'data_1'
LIMIT 3;
-- Should have all columns populated (no NULLs from failed flattening)
```

---

## 3. Silver Layer Validation

### 3a. Total Row Counts and Current/Historical Split

```sql
SELECT
    'operator'  as tbl, COUNT(*) total,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) current_rows,
    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) historical_rows
FROM mfg_silver_db.silver_operator
UNION ALL
SELECT 'machine',  COUNT(*), SUM(CASE WHEN is_current THEN 1 END), SUM(CASE WHEN NOT is_current THEN 1 END) FROM mfg_silver_db.silver_machine
UNION ALL
SELECT 'material', COUNT(*), SUM(CASE WHEN is_current THEN 1 END), SUM(CASE WHEN NOT is_current THEN 1 END) FROM mfg_silver_db.silver_material
UNION ALL
SELECT 'supplier', COUNT(*), SUM(CASE WHEN is_current THEN 1 END), SUM(CASE WHEN NOT is_current THEN 1 END) FROM mfg_silver_db.silver_supplier;
```

**Expected after initial + incremental loads:**

| Table | Total | Current | Historical |
|-------|-------|---------|-----------|
| operator | 13 | 10 | 3 |
| machine | 14 | 9 | 5 |
| material | 14 | 10 | 4 |
| supplier | 10 | 9 | 1 |

### 3b. Verify No Current Row Has effective_to Set

```sql
SELECT COUNT(*) as bad_rows
FROM mfg_silver_db.silver_operator
WHERE is_current = true AND effective_to IS NOT NULL;
-- Expected: 0
```

### 3c. Verify No Historical Row Has NULL effective_to

```sql
SELECT COUNT(*) as bad_rows
FROM mfg_silver_db.silver_operator
WHERE is_current = false AND effective_to IS NULL;
-- Expected: 0
```

### 3d. Verify No Duplicate Current Rows Per PK

```sql
SELECT operator_id, COUNT(*) as versions
FROM mfg_silver_db.silver_operator
WHERE is_current = true
GROUP BY operator_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows (no duplicates)
```

### 3e. Verify Unchanged Records Were Skipped

```sql
-- OP001 should have exactly 1 row (unchanged in data_2)
SELECT operator_id, COUNT(*) as versions
FROM mfg_silver_db.silver_operator
WHERE operator_id IN ('OP001', 'OP004', 'OP005', 'OP006', 'OP008')
GROUP BY operator_id;
-- Expected: all show 1 version (unchanged operators)
```

### 3f. Operation Log: Type Casting Correct

```sql
SELECT
    oplog_id,
    quantity_processed,  -- should be INTEGER
    defects_found,       -- should be INTEGER
    quality_score,       -- should be DECIMAL
    duration_minutes,    -- should be DOUBLE
    defect_rate          -- should be DECIMAL
FROM mfg_silver_db.silver_operation_log
LIMIT 5;

-- Verify types from the schema
DESCRIBE mfg_silver_db.silver_operation_log;
-- quantity_processed: integer
-- defects_found: integer
-- quality_score: decimal(5,2)
-- duration_minutes: double
-- defect_rate: decimal(5,2)
```

### 3g. Verify Defect Rate Formula

```sql
SELECT
    oplog_id,
    quantity_processed,
    defects_found,
    defect_rate,
    -- Recompute manually to verify
    ROUND(CAST(defects_found AS DOUBLE) / quantity_processed * 100, 2) as expected_rate,
    ABS(defect_rate - ROUND(CAST(defects_found AS DOUBLE) / quantity_processed * 100, 2)) as diff
FROM mfg_silver_db.silver_operation_log
WHERE quantity_processed > 0;
-- Expected: diff = 0 for all rows
```

---

## 4. Gold Layer Validation

### 4a. Operation Summary Exists

```sql
SELECT
    report_date,
    total_operations,
    total_quantity,
    total_defects,
    defect_rate_pct,
    avg_quality_score
FROM mfg_gold_db.gold_operation_summary
ORDER BY report_date;
```

### 4b. Verify Aggregation Math

```sql
-- Cross-check Gold summary against Silver source
SELECT
    g.report_date,
    g.total_operations       as gold_ops,
    s.silver_ops,
    g.total_quantity         as gold_qty,
    s.silver_qty,
    g.total_defects          as gold_defects,
    s.silver_defects
FROM mfg_gold_db.gold_operation_summary g
JOIN (
    SELECT
        CAST(start_time AS DATE) as report_date,
        COUNT(*)                 as silver_ops,
        SUM(quantity_processed)  as silver_qty,
        SUM(defects_found)       as silver_defects
    FROM mfg_silver_db.silver_operation_log
    GROUP BY 1
) s ON g.report_date = s.report_date
ORDER BY g.report_date;
-- All gold_ columns should match silver_ columns exactly
```

### 4c. Defect Analysis Row Count

```sql
SELECT COUNT(*) FROM mfg_gold_db.gold_defect_analysis;
-- Should have one row per distinct (report_date, defect_type) combination

SELECT report_date, COUNT(*) as defect_types
FROM mfg_gold_db.gold_defect_analysis
GROUP BY report_date;
```

### 4d. Defect Percentages Sum to 100%

```sql
SELECT
    report_date,
    SUM(defect_pct) as total_pct
FROM mfg_gold_db.gold_defect_analysis
GROUP BY report_date;
-- Expected: approximately 100.0 for each report_date
-- (small floating-point rounding may cause 99.9 or 100.1)
```

### 4e. Supplier Performance Has Valid Ratings

```sql
SELECT
    company_name,
    avg_rating,
    avg_lead_time,
    machine_count,
    avg_quality_score
FROM mfg_gold_db.gold_supplier_performance
ORDER BY avg_rating DESC;

-- Sanity checks:
SELECT
    MIN(avg_rating) as min_rating,
    MAX(avg_rating) as max_rating,
    MIN(avg_lead_time) as min_lead,
    MAX(avg_lead_time) as max_lead,
    MIN(avg_quality_score) as min_quality,
    MAX(avg_quality_score) as max_quality
FROM mfg_gold_db.gold_supplier_performance;
-- Ratings should be 1–5
-- Quality should be 0–100
-- Lead time should be > 0
```

---

## 5. SCD2 Correctness Tests

### Test 1: Changed Operator Has Correct History

```sql
-- OP002: changed workstation from WS-B to WS-C, shift from Morning to Evening
SELECT
    operator_id,
    workstation_id,
    shift,
    effective_from,
    effective_to,
    is_current
FROM mfg_silver_db.silver_operator
WHERE operator_id = 'OP002'
ORDER BY effective_from;
```

**Expected:**
| operator_id | workstation_id | shift | effective_from | effective_to | is_current |
|-------------|---------------|-------|---------------|-------------|-----------|
| OP002 | WS-B | Morning | 2024-01-xx | 2024-02-xx | false |
| OP002 | WS-C | Evening | 2024-02-xx | NULL | true |

### Test 2: Unchanged Operator Has No History

```sql
SELECT operator_id, COUNT(*) as versions
FROM mfg_silver_db.silver_operator
WHERE operator_id = 'OP001'
GROUP BY operator_id;
-- Expected: 1 version (OP001 did not change in data_2)
```

### Test 3: New Record in data_2 Has correct effective_from

```sql
-- OP009 is new in data_2 — should have one row, is_current=true
SELECT operator_id, effective_from, effective_to, is_current
FROM mfg_silver_db.silver_operator
WHERE operator_id IN ('OP009', 'OP010');
-- Expected: is_current = TRUE, effective_to = NULL for both
```

### Test 4: Record Hash Consistency

```sql
-- Verify record_hash is consistent with tracked columns
SELECT
    operator_id,
    record_hash,
    -- Recompute hash
    MD5(CONCAT_WS('|', operator_name, workstation_id, skill_level, shift, status)) as expected_hash
FROM mfg_silver_db.silver_operator
WHERE is_current = true
LIMIT 5;
-- record_hash should equal expected_hash for all rows
```

---

## 6. Data Lineage Spot Checks

### Trace an Operation Log Row Back to Source

```sql
-- Step 1: Pick an operation log entry
SELECT oplog_id, quantity_processed, defects_found, defect_rate
FROM mfg_silver_db.silver_operation_log
WHERE oplog_id = 'OL001';

-- Step 2: Find the Bronze row
SELECT oplog_id, quantity_processed, defects_found, quality_score,
       _source_file, _load_batch, _ingest_timestamp
FROM mfg_bronze_db.bronze_operation_log
WHERE oplog_id = 'OL001';
-- Should show the source file and batch
```

### Verify Batch Filter Isolation

```sql
-- Bronze should only have data_1 rows when load_batch='data_1'
SELECT DISTINCT _load_batch FROM mfg_bronze_db.bronze_operator;
-- Expected: data_1, data_2 (both batches present)

-- Silver should reflect changes across both batches
SELECT MIN(effective_from) as first_load, MAX(effective_from) as last_load
FROM mfg_silver_db.silver_operator;
-- Two distinct timestamps corresponding to the two pipeline runs
```

---

## 7. Iceberg Health Checks

### View Table Snapshots

```sql
-- How many snapshots does silver_operator have?
SELECT COUNT(*) FROM mfg_silver_db."silver_operator$snapshots";
-- Expected: 2 (one per pipeline run that wrote to this table)

-- View snapshot details
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-records']   as added,
    summary['deleted-records'] as deleted
FROM mfg_silver_db."silver_operator$snapshots"
ORDER BY committed_at;
```

### View Data Files

```sql
-- How many Parquet files are in silver_operator?
SELECT COUNT(*) as file_count, SUM(file_size_in_bytes) as total_bytes
FROM mfg_silver_db."silver_operator$files";

-- Files by partition
SELECT partition, COUNT(*) as files, SUM(record_count) as rows
FROM mfg_silver_db."silver_operator$files"
GROUP BY partition;
```

### View Delete Files (SCD2 Creates These)

```sql
-- Delete files are created by MERGE INTO for SCD2 expiry
SELECT file_path, record_count, file_size_in_bytes, content
FROM mfg_silver_db."silver_operator$files"
WHERE content = 1;  -- content=1 means position delete file
-- Expected: 1–3 delete files for the SCD2 expired rows
```

### Check Table History

```sql
SELECT * FROM mfg_silver_db."silver_operator$history"
ORDER BY made_current_at;
```

---

## 8. Expected Results Reference

Use this as your acceptance criteria after a complete two-batch run:

### Bronze Row Counts (Total)
| Table | data_1 | data_2 | Total |
|-------|--------|--------|-------|
| bronze_operator | 8 | 10 | 18 |
| bronze_machine | 8 | 9 | 17 |
| bronze_material | 8 | 10 | 18 |
| bronze_supplier | 8 | 9 | 17 |
| bronze_operation_log | 10 | 5 | 15 |

### Silver Row Counts
| Table | Total Rows | Current | Historical |
|-------|-----------|---------|-----------|
| silver_operator | 13 | 10 | 3 |
| silver_machine | 14 | 9 | 5 |
| silver_material | 14 | 10 | 4 |
| silver_supplier | 10 | 9 | 1 |
| silver_operation_log | 15 | N/A (append only) | N/A |

### Gold Row Counts
| Table | Rows |
|-------|------|
| gold_operation_summary | depends on distinct dates in silver_operation_log |
| gold_defect_analysis | one row per (date × defect_type) combination |
| gold_supplier_performance | one row per (date × supplier_id) combination |

### SCD2 Changed Records
| Entity | Changed PKs | New PKs |
|--------|-------------|---------|
| Operator | OP002, OP003, OP007 | OP009, OP010 |
| Machine | M003, M004, M007, M008 | M009 |
| Material | MAT003, MAT006, MAT008 | MAT009, MAT010 |
| Supplier | 1 existing | 1 new |
