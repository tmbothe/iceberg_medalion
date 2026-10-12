# SCD Type 2 with Apache Iceberg

## Table of Contents
1. [What Are Slowly Changing Dimensions?](#1-what-are-slowly-changing-dimensions)
2. [SCD Types Comparison](#2-scd-types-comparison)
3. [Why SCD Type 2 for This Project?](#3-why-scd-type-2-for-this-project)
4. [SCD2 Schema Design](#4-scd2-schema-design)
5. [SCD2 Logic: Step by Step](#5-scd2-logic-step-by-step)
6. [Implementing SCD2 with Iceberg MERGE INTO](#6-implementing-scd2-with-iceberg-merge-into)
7. [The Record Hash Pattern](#7-the-record-hash-pattern)
8. [Handling the Initial Load vs Incremental Load](#8-handling-the-initial-load-vs-incremental-load)
9. [The MERGE_CARDINALITY_VIOLATION Pitfall](#9-the-merge_cardinality_violation-pitfall)
10. [Worked Example: Operator SCD2](#10-worked-example-operator-scd2)
11. [Composite Primary Keys: Supplier](#11-composite-primary-keys-supplier)

---

## 1. What Are Slowly Changing Dimensions?

In data warehousing and lakehouse architecture, **dimensions** are the descriptive attributes
of your business entities — the "who", "what", and "where" of your fact events.

Examples from this project:
- **Operator**: name, workstation assignment, skill level, shift
- **Machine**: type, supplier, status, last maintenance date
- **Material**: cost, stock quantity, supplier
- **Supplier**: rating, lead time, contact details

These dimensions **change slowly over time** — not every day, but occasionally:
- An operator changes shift from Morning to Afternoon
- A machine gets reassigned to a different supplier
- A supplier's rating improves from 4.2 to 4.7

The question is: **how do you track these changes in your data model?**

---

## 2. SCD Types Comparison

### SCD Type 1 — Overwrite (No History)
Simply overwrite the old value with the new value. History is lost.

```
Before:  OP002 | Bob Smith | WS-B | Senior | Morning
After:   OP002 | Bob Smith | WS-C | Senior | Evening   ← WS-B and Morning are gone
```

Use when: history does not matter; only current state is needed.

---

### SCD Type 2 — Append New Row (Full History)
Keep the old row and add a new row. Use flags/dates to identify the current version.

```
Before:
  OP002 | Bob Smith | WS-B | Senior | Morning | 2024-01-01 | NULL | TRUE

After (old row expired, new row inserted):
  OP002 | Bob Smith | WS-B | Senior | Morning | 2024-01-01 | 2024-02-01 | FALSE  ← expired
  OP002 | Bob Smith | WS-C | Senior | Evening | 2024-02-01 | NULL       | TRUE   ← current
```

Use when: you need to know what an entity looked like at any point in the past.
This is the standard approach for analytical dimensions. ✓ **Used in this project.**

---

### SCD Type 3 — Add Previous Value Column
Add a column to store the previous value of a specific attribute.

```
OP002 | Bob Smith | WS-C | WS-B | Senior | Evening | Morning
                     ↑ current  ↑ previous
```

Use when: you only care about one previous value and only for one attribute.
Rarely used in practice.

---

### SCD Type 6 — Hybrid (1+2+3)
Combines all three: overwrite current value, add new row, add previous-value column.
Complex to implement and query.

---

## 3. Why SCD Type 2 for This Project?

The Gold layer reports need to answer questions like:
- "What was this operator's shift assignment when this operation log was recorded?"
- "Was this machine in Active status on 2024-01-15?"

If we used SCD Type 1 (overwrite), we could never answer these questions — the history
would be gone.

SCD Type 2 preserves every version of every dimension row, so we can always join a
fact (operation log) to the dimension as it existed at that point in time:

```sql
SELECT
    op.operator_name,
    op.shift,     -- What shift were they on at the time of the operation?
    ol.start_time,
    ol.defect_rate
FROM silver_operation_log ol
JOIN silver_operator op
  ON ol.operator_id = op.operator_id
 AND op.is_current = false  -- Or: op.effective_from <= ol.start_time AND
                             --     (op.effective_to IS NULL OR op.effective_to > ol.start_time)
```

---

## 4. SCD2 Schema Design

Every SCD2 silver table has these additional columns beyond the business attributes:

| Column | Type | Description |
|--------|------|-------------|
| `effective_from` | TIMESTAMP | When this version became active |
| `effective_to` | TIMESTAMP | When this version was superseded (NULL = still current) |
| `is_current` | BOOLEAN | Quick flag: TRUE = current version |
| `record_hash` | STRING | MD5 hash of tracked columns — used to detect changes |
| `_created_at` | TIMESTAMP | When this row was first written to Silver |
| `_updated_at` | TIMESTAMP | When this row was last modified (set on expiry) |

### Why `is_current` in addition to `effective_to IS NULL`?

Both convey the same information, but `is_current` is simpler to filter:
```sql
-- With is_current:
WHERE is_current = true

-- Without is_current:
WHERE effective_to IS NULL
```

Also, `is_current` is the partition column — partitioning on it means queries for
current records only read the `is_current=true` partition files, skipping all history.

---

## 5. SCD2 Logic: Step by Step

```
Input: incoming records from Bronze for this load batch
Target: existing Silver table

Step 1: Deduplicate incoming by PK
  → Guard against duplicate source records causing MERGE violations

Step 2: Compute record_hash for each incoming row
  → MD5 of all tracked columns concatenated
  → If hash matches existing current row: no change needed

Step 3: Join incoming to existing current rows
  → Left join on PK: incoming LEFT JOIN silver WHERE is_current = true

Step 4: Categorize each incoming row:
  → NEW:       no matching current row in Silver (new PK)
  → CHANGED:   matching current row exists, but record_hash differs
  → UNCHANGED: matching current row exists, record_hash is identical

Step 5: For CHANGED rows
  → MERGE INTO Silver:
      WHEN MATCHED (PK + is_current=true) THEN UPDATE:
        is_current = false
        effective_to = NOW
        _updated_at = NOW

Step 6: For NEW + CHANGED rows
  → INSERT into Silver:
      effective_from = NOW
      effective_to = NULL
      is_current = true
      _created_at = NOW
      _updated_at = NOW

Step 7: UNCHANGED rows are ignored (no write)
```

---

## 6. Implementing SCD2 with Iceberg MERGE INTO

The key SQL statement for expiring changed rows:

```sql
MERGE INTO glue_catalog.mfg_silver_db.silver_operator t
USING changed_pks s
ON t.operator_id = s.operator_id AND t.is_current = true
WHEN MATCHED THEN UPDATE SET
    t.is_current   = false,
    t.effective_to = current_timestamp(),
    t._updated_at  = current_timestamp()
```

`changed_pks` is a temporary view containing just the PKs of rows that changed:

```python
changed.select(pk_col).distinct().createOrReplaceTempView("changed_pks")
```

**Why `.distinct()` on the source?** Iceberg's MERGE requires that each target row matches
at most one source row. If `changed_pks` has duplicate PKs, Iceberg raises
`MERGE_CARDINALITY_VIOLATION`. The `.distinct()` guarantees uniqueness.

Then, inserting new current versions:

```python
to_insert = new_recs.union(changed) \
    .drop("_ex_hash", "_exists") \
    .dropDuplicates([pk_col]) \
    .withColumn("effective_from", NOW) \
    .withColumn("effective_to", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("_created_at", NOW) \
    .withColumn("_updated_at", NOW)

to_insert.writeTo(silver_table).append()
```

---

## 7. The Record Hash Pattern

To detect whether a row has changed, we compute an MD5 hash of all "tracked" columns
(columns we care about for change detection, typically excluding audit columns):

```python
incoming = incoming_df.dropDuplicates([pk_col]).withColumn(
    "record_hash",
    md5(concat_ws("|", *[col(c) for c in track_cols]))
)
```

For the Operator table, `track_cols` is:
```python
["operator_name", "workstation_id", "skill_level", "shift", "status"]
```

Note that `hire_date` is **not** in track_cols — it never changes after hire.

**How it works:**
```
Row: OP002 | Bob Smith | WS-B | Senior | Morning | Active
Hash: md5("Bob Smith|WS-B|Senior|Morning|Active") = "a3f4e2..."

Row changes: OP002 | Bob Smith | WS-C | Senior | Evening | Active
Hash: md5("Bob Smith|WS-C|Senior|Evening|Active") = "7b9d1e..."

a3f4e2 ≠ 7b9d1e → row is CHANGED → apply SCD2
```

The hash is stored in the silver table and reused in subsequent incremental loads:

```python
existing = spark.table(silver_table).filter("is_current = true")
# existing already has record_hash from the previous run
# We compare incoming.record_hash to existing.record_hash
```

---

## 8. Handling the Initial Load vs Incremental Load

The PySpark `run_scd2()` function in `06_bronze_to_silver.py` handles both cases:

### Initial Load (`LOAD_TYPE = 'initial'`)

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

All incoming rows are written as current versions. No comparison needed.
`createOrReplace()` overwrites any existing table content (safe for first run).

### Incremental Load (`LOAD_TYPE = 'incremental'`)

```python
else:
    existing = spark.table(silver_table).filter("is_current = true")
    # ... join, categorize, expire, insert ...
```

Reads only current rows from Silver (partition pruning: `is_current=true` partition only),
then applies the full SCD2 logic.

---

## 9. The MERGE_CARDINALITY_VIOLATION Pitfall

**Error message:**
```
MERGE_CARDINALITY_VIOLATION: The ON search condition of the MERGE statement matched
a single row from the target table with multiple rows of the source table.
```

**Cause:** Iceberg's MERGE INTO requires that each target row matches **at most one**
source row. If the source has duplicate values in the join key, Iceberg rejects the operation.

**Where it can occur in this project:**
1. `changed_pks` has duplicate PKs → fixed by `.distinct()`
2. `incoming_df` has duplicate PKs (e.g., same operator appears twice in the CSV) → fixed by `dropDuplicates([pk_col])`
3. `to_insert` has duplicate PKs before append → fixed by `.dropDuplicates([pk_col])`

**All three are guarded in the code:**
```python
# Guard 1: deduplicate incoming before any comparison
incoming = incoming_df.dropDuplicates([pk_col]).withColumn("record_hash", ...)

# Guard 2: deduplicate MERGE source
changed.select(pk_col).distinct().createOrReplaceTempView("changed_pks")

# Guard 3: deduplicate before append
to_insert = new_recs.union(changed) \
    .drop("_ex_hash", "_exists") \
    .dropDuplicates([pk_col]) \   ← Guard 3
    ...
```

---

## 10. Worked Example: Operator SCD2

### data_1 (initial load)
```
Source:
  OP001 | Alice Johnson  | WS-A | Expert | Morning | Active
  OP002 | Bob Smith      | WS-B | Senior | Morning | Active
  ...

Silver after initial load (all 8 rows, all is_current=true):
  OP001 | Alice | WS-A | Expert | Morning | Active | 2024-01-01 | NULL | TRUE  | hash_1
  OP002 | Bob   | WS-B | Senior | Morning | Active | 2024-01-01 | NULL | TRUE  | hash_2
  ...
```

### data_2 (incremental load)
```
Source:
  OP001 | Alice Johnson  | WS-A | Expert | Morning  | Active    ← UNCHANGED (same hash)
  OP002 | Bob Smith      | WS-C | Senior | Evening  | Active    ← CHANGED (WS-B→WS-C, shift)
  OP003 | Carol Davis    | WS-D | Expert | Morning  | On Leave  ← CHANGED (status change)
  OP007 | Grace Wilson   | WS-G | Expert | Night    | Active    ← CHANGED (shift change)
  OP009 | Ivy Taylor     | WS-I | Junior | Morning  | Active    ← NEW
  OP010 | Jack Anderson  | WS-J | Trainee| Afternoon| Active    ← NEW
  (OP004-OP006, OP008 are also unchanged, not in data_2 — ignored)
```

```
Step 1: compute hashes for incoming rows
Step 2: join with existing current rows
  OP001: hash_same → UNCHANGED → skip
  OP002: hash_diff → CHANGED
  OP003: hash_diff → CHANGED
  OP007: hash_diff → CHANGED
  OP009: no existing → NEW
  OP010: no existing → NEW

Step 3: MERGE INTO to expire changed rows
  UPDATE silver WHERE operator_id IN ('OP002','OP003','OP007') AND is_current=true
    SET is_current=false, effective_to=NOW

Step 4: INSERT new current rows for OP002, OP003, OP007, OP009, OP010

Silver after incremental load (13 rows total):
  OP001 | Alice | WS-A | Expert | Morning  | Active   | 2024-01-01 | NULL       | TRUE  ← current
  OP002 | Bob   | WS-B | Senior | Morning  | Active   | 2024-01-01 | 2024-02-01 | FALSE ← expired
  OP002 | Bob   | WS-C | Senior | Evening  | Active   | 2024-02-01 | NULL       | TRUE  ← current
  OP003 | Carol | WS-C | Expert | Morning  | Active   | 2024-01-01 | 2024-02-01 | FALSE ← expired
  OP003 | Carol | WS-D | Expert | Morning  | On Leave | 2024-02-01 | NULL       | TRUE  ← current
  OP007 | Grace | WS-G | Expert | Morning  | Active   | 2024-01-01 | 2024-02-01 | FALSE ← expired
  OP007 | Grace | WS-G | Expert | Night    | Active   | 2024-02-01 | NULL       | TRUE  ← current
  OP009 | Ivy   | WS-I | Junior | Morning  | Active   | 2024-02-01 | NULL       | TRUE  ← new
  OP010 | Jack  | WS-J | Trainee| Afternoon| Active   | 2024-02-01 | NULL       | TRUE  ← new
  ... (OP004–OP006, OP008 unchanged, still have their original rows)
```

Result: **13 rows** in silver_operator (10 current + 3 historical).
This matches the Athena validation result.

---

## 11. Composite Primary Keys: Supplier

The Supplier table has a **composite primary key** (`supplier_id` + `supplier_machine_id`)
because one supplier can supply multiple machines, and each combination is a separate record.

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
    pk_col  = "supplier_pk",   # ← "SUP001~M001", "SUP001~M002", etc.
    track_cols = ["company_name", "country", "contact_email", "lead_time_days", "rating"],
    ...
)
```

The synthetic key `supplier_pk` (`SUP001~M001`) is stored in the Silver table and used as
the join/dedup key. The original `supplier_id` and `supplier_machine_id` are preserved as
separate columns for downstream joins.
