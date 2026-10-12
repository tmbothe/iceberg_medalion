# Gold Layer — Aggregated Reports

## Table of Contents
1. [Gold Layer Purpose](#1-gold-layer-purpose)
2. [Gold Tables Overview](#2-gold-tables-overview)
3. [Report 1: gold_operation_summary](#3-report-1-gold_operation_summary)
4. [Report 2: gold_defect_analysis](#4-report-2-gold_defect_analysis)
5. [Report 3: gold_supplier_performance](#5-report-3-gold_supplier_performance)
6. [The align_to_table() Helper](#6-the-align_to_table-helper)
7. [Overwrite Pattern and Idempotency](#7-overwrite-pattern-and-idempotency)
8. [Running the Gold Job](#8-running-the-gold-job)
9. [Querying Gold via Athena](#9-querying-gold-via-athena)

---

## 1. Gold Layer Purpose

Gold is the **business-ready serving layer**. Its sole purpose is to answer business
questions quickly and simply.

Gold tables are:
- **Pre-aggregated** — expensive GROUP BY and JOIN operations are done once at load time
- **Pre-joined** — dimension attributes (operator names, supplier names) are already embedded
- **Daily-partitioned** — partition by `report_date` so queries for a date range are efficient
- **Overwritten daily** — running the Gold job twice for the same date produces the same result

**Who consumes Gold:**
- Business analysts via Amazon Athena
- BI dashboards (Grafana, QuickSight, Tableau)
- Scheduled reports
- Data scientists doing exploratory analysis

---

## 2. Gold Tables Overview

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `gold_operation_summary` | Per day (`report_date`) | Total ops, qty, defects, defect rate, avg quality |
| `gold_defect_analysis` | Per day × defect type | Count, percentage, rank |
| `gold_supplier_performance` | Per day × supplier | Avg rating, avg lead time, machine count, avg quality |

All three tables:
- Are Iceberg format with `format-version=2`
- Are partitioned by `report_date`
- Are written with `overwritePartitions()` — daily re-runs are safe and idempotent

---

## 3. Report 1: gold_operation_summary

**Business question:** "How did the production line perform on each day?"

### Schema
```
report_date          DATE         ← partition column
total_operations     BIGINT       ← count of operations
total_quantity       BIGINT       ← sum of quantity_processed
total_defects        BIGINT       ← sum of defects_found
defect_rate_pct      DECIMAL(5,2) ← (total_defects / total_quantity) * 100
avg_quality_score    DECIMAL(5,2) ← average quality_score
```

### How it's built

```python
op_summary = silver_log \
    .withColumn("report_date", to_date(col("start_time"))) \
    .groupBy("report_date") \
    .agg(
        count("oplog_id").alias("total_operations"),
        sum("quantity_processed").alias("total_quantity"),
        sum("defects_found").alias("total_defects"),
        round(
            sum("defects_found").cast("double") /
            when(sum("quantity_processed") > 0, sum("quantity_processed")).otherwise(1) * 100,
            2
        ).alias("defect_rate_pct"),
        round(avg("quality_score"), 2).alias("avg_quality_score")
    )
```

### Example Output
```
report_date | total_ops | total_qty | total_defects | defect_rate | avg_quality
2024-01-15  | 3         | 425       | 10            | 2.35%       | 97.33
2024-01-16  | 2         | 350       | 7             | 2.00%       | 96.75
```

### Athena Query
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

---

## 4. Report 2: gold_defect_analysis

**Business question:** "Which types of defects are most common? What percentage do they represent?"

### Schema
```
report_date        DATE
defect_type        STRING       ← "Surface Scratch", "Misalignment", etc.
defect_count       BIGINT       ← count of operations with this defect type
total_defects_day  BIGINT       ← total defects on this day (for percentage calc)
defect_pct         DECIMAL(5,2) ← defect_count / total_defects_day * 100
defect_rank        INTEGER      ← rank 1 = most common defect type
```

### How it's built

The percentage requires knowing the total defects for the day — this uses a window function:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, rank

window_day = Window.partitionBy("report_date")

defect_df = silver_log \
    .filter(col("defect_type").isNotNull() & (col("defect_type") != "")) \
    .withColumn("report_date", to_date(col("start_time"))) \
    .groupBy("report_date", "defect_type") \
    .agg(count("oplog_id").alias("defect_count")) \
    .withColumn(
        "total_defects_day",
        spark_sum("defect_count").over(window_day)   ← window: total for the day
    ) \
    .withColumn(
        "defect_pct",
        round(col("defect_count").cast("double") / col("total_defects_day") * 100, 2)
    ) \
    .withColumn(
        "defect_rank",
        rank().over(window_day.orderBy(col("defect_count").desc()))
    )
```

### Example Output
```
report_date | defect_type        | count | total | pct   | rank
2024-01-15  | Surface Scratch    | 5     | 10    | 50.0% | 1
2024-01-15  | Misalignment       | 3     | 10    | 30.0% | 2
2024-01-15  | Dimensional Error  | 2     | 10    | 20.0% | 3
```

### Athena Query
```sql
SELECT
    report_date,
    defect_type,
    defect_count,
    defect_pct,
    defect_rank
FROM mfg_gold_db.gold_defect_analysis
ORDER BY report_date, defect_rank;
```

---

## 5. Report 3: gold_supplier_performance

**Business question:** "Which suppliers are performing best? What is each supplier's contribution
to quality and delivery reliability?"

### Schema
```
report_date       DATE
supplier_id       STRING
company_name      STRING       ← from silver_supplier (current version)
country           STRING
avg_rating        DECIMAL(5,2) ← average supplier rating
avg_lead_time     DECIMAL(5,2) ← average lead time in days
machine_count     BIGINT       ← how many machines this supplier provides
avg_quality_score DECIMAL(5,2) ← average quality from operations using their machines
```

### How it's built

This requires joining three Silver tables:
1. `silver_operation_log` — the operations (quality scores)
2. `silver_machine` — to link operations to supplier via `machine_id`
3. `silver_supplier` — to get supplier details (name, rating, lead time)

```python
# Get current versions only (is_current=true)
machines  = spark.table("glue_catalog.mfg_silver_db.silver_machine").filter("is_current=true")
suppliers = spark.table("glue_catalog.mfg_silver_db.silver_supplier").filter("is_current=true")

supplier_perf = silver_log \
    .withColumn("report_date", to_date(col("start_time"))) \
    .join(machines.select("machine_id", "supplier_id"), "machine_id", "left") \
    .join(
        suppliers.select(
            "supplier_id", "company_name", "country",
            col("lead_time_days").cast("double"),
            col("rating").cast("double")
        ),
        "supplier_id", "left"
    ) \
    .groupBy("report_date", "supplier_id", "company_name", "country") \
    .agg(
        round(avg("rating"), 2).alias("avg_rating"),
        round(avg("lead_time_days"), 2).alias("avg_lead_time"),
        countDistinct("machine_id").alias("machine_count"),
        round(avg("quality_score"), 2).alias("avg_quality_score")
    )
```

### Why join to `is_current=true` versions of dimensions?

For Gold reports, we typically want to report on the **current state** of dimensions
(who is the operator right now, what is the current supplier rating). If you need
"what was the supplier rating at the time of the operation", you would do a temporal join:

```sql
-- Temporal join (point-in-time): get the supplier state at operation time
JOIN silver_supplier s
  ON m.supplier_id = s.supplier_id
 AND s.effective_from <= ol.start_time
 AND (s.effective_to IS NULL OR s.effective_to > ol.start_time)
```

For this project, we use current versions for simplicity. The temporal join approach
is documented in [04_scd_type2.md](04_scd_type2.md).

### Example Output
```
report_date | supplier_id | company_name  | country | avg_rating | avg_lead | machines | avg_quality
2024-01-15  | SUP001      | TechSupply Co | USA     | 4.50       | 7.0      | 3        | 97.5
2024-01-15  | SUP002      | GlobalParts   | Germany | 4.30       | 10.0     | 2        | 96.2
```

### Athena Query
```sql
SELECT
    report_date,
    company_name,
    country,
    avg_rating,
    avg_lead_time,
    machine_count,
    avg_quality_score
FROM mfg_gold_db.gold_supplier_performance
ORDER BY report_date, avg_rating DESC;
```

---

## 6. The `align_to_table()` Helper

The Gold job includes a utility function to align a DataFrame's columns to match an
existing Iceberg table's schema:

```python
def align_to_table(df, table_name):
    """
    Reorders and selects columns from df to match the schema of an existing Iceberg table.
    Prevents 'cannot resolve column' errors when column order differs.
    """
    target_schema = spark.table(table_name).schema
    cols = [col(f.name).cast(f.dataType) for f in target_schema.fields
            if f.name in [c for c in df.columns]]
    return df.select(cols)
```

This is important because Iceberg's `writeTo().overwritePartitions()` requires the DataFrame
columns to match the table schema exactly (by name and type). If the Gold aggregation query
produces columns in a different order, `align_to_table()` reorders them.

---

## 7. Overwrite Pattern and Idempotency

Gold tables use `overwritePartitions()` instead of `append()`:

```python
gold_df.writeTo("glue_catalog.mfg_gold_db.gold_operation_summary") \
    .overwritePartitions()
```

`overwritePartitions()` replaces **only the partitions present in the new data**.
If today's `report_date=2024-01-15` data is re-processed, only the `2024-01-15` partition
is overwritten. Historical partitions (e.g., `report_date=2024-01-14`) are untouched.

This means:
- Re-running the Gold job for the same date is **safe** (no duplicates)
- Running Gold for a new date **adds** a new partition without touching existing ones
- If upstream Silver data changes, re-running Gold picks up the changes automatically

---

## 8. Running the Gold Job

```bash
aws glue start-job-run \
    --job-name mfg-silver-to-gold \
    --arguments '{
        "--REPORT_DATE":"2024-02-02",
        "--BUCKET_NAME":"mfg-lakehouse-442042510197-us-east-1"
    }'
```

The `REPORT_DATE` parameter controls which date of `silver_operation_log` is aggregated:
```python
silver_log = spark.table("glue_catalog.mfg_silver_db.silver_operation_log") \
    .filter(f"CAST(start_time AS DATE) <= DATE '{REPORT_DATE}'")
```

This aggregates all operations up to and including `REPORT_DATE`.

---

## 9. Querying Gold via Athena

### Full Daily Performance Report

```sql
SELECT
    s.report_date,
    s.total_operations,
    s.total_quantity,
    s.total_defects,
    s.defect_rate_pct,
    s.avg_quality_score
FROM mfg_gold_db.gold_operation_summary s
ORDER BY s.report_date;
```

### Top Defects This Week

```sql
SELECT
    defect_type,
    SUM(defect_count) as total_defects,
    ROUND(AVG(defect_pct), 2) as avg_pct
FROM mfg_gold_db.gold_defect_analysis
WHERE report_date >= DATE '2024-01-15'
GROUP BY defect_type
ORDER BY total_defects DESC
LIMIT 5;
```

### Supplier Leaderboard

```sql
SELECT
    company_name,
    country,
    ROUND(AVG(avg_rating), 2) as overall_rating,
    ROUND(AVG(avg_lead_time), 1) as avg_lead_days,
    ROUND(AVG(avg_quality_score), 2) as quality
FROM mfg_gold_db.gold_supplier_performance
GROUP BY company_name, country
ORDER BY overall_rating DESC;
```

### Combined Dashboard Query

```sql
SELECT
    s.report_date,
    s.total_operations,
    s.defect_rate_pct,
    s.avg_quality_score,
    d.defect_type as top_defect,
    p.company_name as top_supplier
FROM mfg_gold_db.gold_operation_summary s
LEFT JOIN (
    SELECT report_date, defect_type
    FROM mfg_gold_db.gold_defect_analysis
    WHERE defect_rank = 1
) d ON s.report_date = d.report_date
LEFT JOIN (
    SELECT report_date, company_name,
           ROW_NUMBER() OVER (PARTITION BY report_date ORDER BY avg_rating DESC) as rn
    FROM mfg_gold_db.gold_supplier_performance
) p ON s.report_date = p.report_date AND p.rn = 1
ORDER BY s.report_date;
```
