# Problem Statement

## Table of Contents
1. [Business Context](#1-business-context)
2. [The Assembly Line Domain](#2-the-assembly-line-domain)
3. [Data Sources and Schema](#3-data-sources-and-schema)
4. [Business Requirements](#4-business-requirements)
5. [Why Not a Traditional Data Warehouse?](#5-why-not-a-traditional-data-warehouse)
6. [Why a Lakehouse?](#6-why-a-lakehouse)

---

## 1. Business Context

A small manufacturing company operates a physical assembly line producing goods —
think consumer electronics, automotive components, or industrial parts.
The line runs continuously across multiple **workstations**, each staffed by one or more **operators**
who use specific **machines** and **materials** to perform assembly operations.

The company wants to answer operational questions such as:

- Which operators have the highest defect rates this week?
- Which machines are producing the most defects?
- Which suppliers are delivering materials with the most quality issues?
- How efficient is each workstation on a given day?
- When did Operator OP003 change shift assignments? (historical tracking)

Currently these questions are answered by manually querying production databases, which is slow,
error-prone, and does not preserve history.

---

## 2. The Assembly Line Domain

```
                 ┌─────────────────────────────────────────────────┐
                 │              Assembly Floor                       │
                 │                                                   │
  Suppliers ────►│  Materials ──► [WS1] ──► [WS2] ──► [WS3] ──►   │──► Finished Product
                 │                  ↑          ↑          ↑          │
  Suppliers ────►│  Machines  ──────┴──────────┴──────────┘          │
                 │                                                   │
                 │  Operators are assigned to each workstation        │
                 └─────────────────────────────────────────────────┘
```

### Key Entities

| Entity | Description | Source Format |
|--------|-------------|---------------|
| **Operator** | A person assigned to a workstation | CSV |
| **Machine** | Equipment used in operations | CSV |
| **Material** | Raw / semi-finished input consumed | CSV |
| **Supplier** | Company providing machines or materials | JSON (nested array) |
| **Operation Log** | Record of every operation performed | JSON (nested object) |

### How They Relate

```
Supplier
  └── provides ──► Machine
  └── provides ──► Material

Operator
  └── works at ──► Workstation

Operation Log (fact)
  └── references ──► Operator
  └── references ──► Machine
  └── references ──► Material
  └── references ──► Workstation
  └── references ──► Work Order
  └── records:  quantity_processed, defects_found, quality_score, duration
```

An **Operation Log** entry (`oplog`) is created every time an operator performs a task.
It is the central fact table — everything else is a dimension that describes the context.

---

## 3. Data Sources and Schema

### Operator (CSV)
```
operator_id, operator_name, workstation_id, skill_level, hire_date, shift, status
OP001, Alice Johnson, WS-A, Expert, 2019-03-15, Morning, Active
OP002, Bob Smith,     WS-B, Senior, 2020-07-22, Afternoon, Active
```

### Machine (CSV)
```
machine_id, machine_name, machine_type, supplier_id, status, purchase_date,
capacity_per_hour, last_maintenance_date
M001, CNC Mill Alpha, CNC Mill, SUP001, Active, 2021-06-10, 50, 2024-01-05
```

### Material (CSV)
```
material_id, material_name, material_type, supplier_id, unit_cost,
unit_of_measure, stock_quantity, reorder_level
MAT001, Steel Rod Type A, Raw Material, SUP001, 12.50, kg, 5000, 500
```

### Supplier (JSON — nested array)
```json
{
  "supplier_id": "SUP001",
  "supplier_machine_id": "M001",
  "supplier_details": [
    {"detail_key": "company_name",  "detail_value": "TechSupply Co"},
    {"detail_key": "country",       "detail_value": "USA"},
    {"detail_key": "contact_email", "detail_value": "contact@techsupply.com"},
    {"detail_key": "lead_time_days","detail_value": "7"},
    {"detail_key": "rating",        "detail_value": "4.5"}
  ]
}
```
The `supplier_details` is a **key-value array** — the pipeline must pivot this into columns.

### Operation Log (JSON — nested object)
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
The `work_order_details` is an **embedded object** — the pipeline must flatten it.

---

## 4. Business Requirements

### Functional Requirements

**UC-1 — Initial Load**
Load all entities from `data_1` (first snapshot of the production system) into the lakehouse.
All records should be inserted as new.

**UC-2 — Incremental Load with History**
Load updated/new records from `data_2`. Apply the following rules:
- **New records** → insert
- **Changed records** → expire the old version, insert the new version (SCD Type 2)
- **Unchanged records** → ignore (no duplicate rows)

**UC-3 — Report: Daily Operation Summary**
Per day: total operations, total quantity, total defects, overall defect rate, average quality score.

**UC-4 — Report: Defect Analysis**
Per defect type: count and percentage of total defects. Rank by frequency.

**UC-5 — Report: Supplier Performance**
Per supplier: average rating, average lead time, count of associated machines, average quality score
from operations that used their machines.

### Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| Cost | Minimize — no always-on cluster |
| History | Full history of dimension changes (SCD2) |
| Idempotency | Re-running the same batch produces the same result |
| Queryability | All layers queryable via SQL (Athena) |
| Auditability | Every row traceable to its source batch and ingest time |

---

## 5. Why Not a Traditional Data Warehouse?

A managed data warehouse (Amazon Redshift, Snowflake, Google BigQuery) would solve the
querying and history requirements, but at significant cost:

| Factor | Data Warehouse | Lakehouse |
|--------|---------------|-----------|
| Compute cost | Always-on cluster ≥ $200/month | Pay-per-use (Glue + Athena) |
| Storage cost | Proprietary storage ($23/TB/month) | S3 ($23/TB/month + table format) |
| Vendor lock-in | High — proprietary SQL dialects | Low — open format (Iceberg) |
| Scale ceiling | Limited by cluster size | Virtually unlimited (S3) |
| Flexibility | Schema-on-write | Schema-on-read + evolution |

For a small company with modest data volumes, **lakehouse architecture achieves the same
analytical capability at a fraction of the cost.**

---

## 6. Why a Lakehouse?

A **data lakehouse** combines the low-cost, scalable storage of a data lake with the
ACID transactions and SQL querying capabilities of a data warehouse.

The key enabler is an **open table format** (Apache Iceberg, in our case) that sits on top of
plain S3 files and adds:

- **ACID transactions** — no partial writes or inconsistent reads
- **Schema evolution** — add/rename columns without rewriting data
- **Time travel** — query any historical snapshot
- **Partition evolution** — change partitioning without rewriting data
- **Merge operations** — `MERGE INTO` for SCD2 and upserts

This means we get data-warehouse-level capabilities with data-lake-level costs.

```
Data Lake      =  S3 + cheap storage + no ACID + no schema enforcement
Data Warehouse =  expensive cluster + ACID + SQL + schema enforcement
Data Lakehouse =  S3 + open table format (Iceberg) + ACID + SQL + schema evolution
```

See [docs/02_medallion_architecture.md](02_medallion_architecture.md) for how we structure
the data, and [docs/03_apache_iceberg.md](03_apache_iceberg.md) for a deep dive on Iceberg.
