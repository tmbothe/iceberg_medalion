# AWS Lake Formation — Setup, Users & Permissions

## Table of Contents
1. [What is Lake Formation?](#1-what-is-lake-formation)
2. [Lake Formation vs. IAM-Only Access](#2-lake-formation-vs-iam-only-access)
3. [Core Concepts](#3-core-concepts)
4. [Initial Lake Formation Setup](#4-initial-lake-formation-setup)
5. [Register the S3 Location](#5-register-the-s3-location)
6. [Create Databases and Grant Database Permissions](#6-create-databases-and-grant-database-permissions)
7. [Grant Table-Level Permissions](#7-grant-table-level-permissions)
8. [Creating Users with Different Access Levels](#8-creating-users-with-different-access-levels)
9. [Lake Formation + Iceberg: Important Notes](#9-lake-formation--iceberg-important-notes)
10. [Permission Hierarchy Diagram](#10-permission-hierarchy-diagram)
11. [Troubleshooting Common Errors](#11-troubleshooting-common-errors)

---

## 1. What is Lake Formation?

AWS Lake Formation is a service that makes it easy to set up, secure, and manage a
data lake. It sits on top of S3 and the Glue Data Catalog to provide:

- **Centralized access control** — manage who can query which tables from one place
- **Column-level security** — restrict access to specific columns (e.g., hide PII)
- **Row-level security** — filter rows based on user identity (data filters)
- **Tag-based access control** — attach security tags to databases/tables/columns
- **Data lake formation** — register S3 paths and manage cross-account sharing

Without Lake Formation, access to Glue tables is controlled purely by IAM policies on
the S3 bucket (check if the role can `s3:GetObject`) and Glue API (check if the role can
`glue:GetTable`). With Lake Formation, there is an **additional** permission layer that
says whether a principal can SELECT/INSERT/DELETE on a specific table.

---

## 2. Lake Formation vs. IAM-Only Access

### Before Lake Formation (IAM-only)
```
User wants to SELECT from mfg_gold_db.gold_operation_summary
  ↓
Athena checks: does this user have s3:GetObject on the bucket?  → IAM policy
Athena checks: does this user have glue:GetTable?              → IAM policy
If both: allow
```

Simple, but no table-level granularity. If you have S3 access, you can read any table
in any prefix.

### With Lake Formation
```
User wants to SELECT from mfg_gold_db.gold_operation_summary
  ↓
Lake Formation checks: does this user have SELECT on this specific table? → LF permissions
  AND
Athena checks: does this user have s3:GetObject on the bucket?            → IAM policy
If both: allow
```

This gives you table-level (and even column-level) access control without managing
hundreds of S3 bucket policies.

> **Key insight:** Lake Formation is **additive** to IAM. You need BOTH Lake Formation
> permissions AND IAM permissions. Lake Formation cannot grant more than IAM allows.

---

## 3. Core Concepts

### Data Lake Administrator
A Lake Formation admin can:
- Register S3 locations
- Create and manage LF permissions on databases/tables
- Grant/revoke permissions to other principals
- Run DROP TABLE on Iceberg tables (critical for maintenance operations)

The first admin must be set via the AWS root account or a user with `lakeformation:PutDataLakeSettings`.

### Resource Link
A pointer to a shared database in another account. Not used in this project.

### LF-Tags (Tag-Based Access Control)
Instead of granting access per-table, you can attach tags (e.g., `classification=gold, team=analytics`)
to tables, then grant access based on tags. More scalable than per-table grants.

### Data Filters
Row-level filters that restrict which rows a user can see. Applied at query time by the
Athena/Glue engine. Not used in this project but useful for multi-tenant scenarios.

### Registered Location
An S3 prefix registered with Lake Formation. Lake Formation manages access to data in
registered locations. If a path is NOT registered, Lake Formation falls back to IAM.

---

## 4. Initial Lake Formation Setup

### Step 1: Enable Lake Formation (First Time Only)

1. Go to **AWS Console → Lake Formation**
2. If it's the first time: click **Get started** and acknowledge the initial setup
3. Under **Administration → Data lake administrators**, add your admin user/role

Via CLI:
```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ADMIN_USER_ARN="arn:aws:iam::${ACCOUNT_ID}:user/mfg-dev-user"
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

aws lakeformation put-data-lake-settings \
    --data-lake-settings "{
        \"DataLakeAdmins\": [
            {\"DataLakePrincipalIdentifier\": \"${ADMIN_USER_ARN}\"},
            {\"DataLakePrincipalIdentifier\": \"${GLUE_ROLE_ARN}\"}
        ],
        \"CreateDatabaseDefaultPermissions\": [],
        \"CreateTableDefaultPermissions\": []
    }"
```

> **Important:** Setting `CreateDatabaseDefaultPermissions` and `CreateTableDefaultPermissions`
> to empty arrays disables the legacy IAM-based access fallback. This is best practice for
> Lake Formation-governed environments — all access is explicitly granted.
>
> If you leave these with `AllUsers: ALL`, then any IAM principal with S3+Glue permissions
> can access all tables, bypassing Lake Formation table-level control.

### Step 2: Add Glue Role as Lake Formation Admin

The Glue ETL role needs to be a Lake Formation admin so it can:
- Create tables in registered databases
- Drop tables (for cleanup/recreation operations)
- Grant LF permissions to itself during job initialization

```bash
# The data-lake-settings shown above already includes the Glue role.
# Verify with:
aws lakeformation get-data-lake-settings \
    --query 'DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier'
```

---

## 5. Register the S3 Location

Lake Formation must know which S3 paths it governs.

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-us-east-1"
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

# Register the entire bucket
aws lakeformation register-resource \
    --resource-arn "arn:aws:s3:::${BUCKET_NAME}" \
    --role-arn "${GLUE_ROLE_ARN}" \
    --use-service-linked-role false

# Verify registration
aws lakeformation list-resources
```

Once registered, Lake Formation uses the `GLUE_ROLE_ARN` for cross-service S3 access.
Athena will use the user's IAM role to query Glue, but the actual S3 read is done via
the registered role (vended credentials pattern).

---

## 6. Create Databases and Grant Database Permissions

After creating Glue databases (via `2_create_glue_catalog.sh`), grant the Glue role
permission to create tables in those databases:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

for DB in mfg_bronze_db mfg_silver_db mfg_gold_db; do
    echo "Granting permissions on ${DB}..."

    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
        --resource "{\"Database\":{\"Name\":\"${DB}\"}}" \
        --permissions "CREATE_TABLE" "ALTER" "DROP" "DESCRIBE"
done
```

This allows the Glue job to create Iceberg tables in these databases during the first run.

---

## 7. Grant Table-Level Permissions

### Grant Glue Role Full Table Permissions

The Glue role needs to read/write/drop all tables for ETL and cleanup:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

# Grant on all tables in a database
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
    --resource '{
        "Table": {
            "DatabaseName": "mfg_bronze_db",
            "TableWildcard": {}
        }
    }' \
    --permissions "SELECT" "INSERT" "DELETE" "ALTER" "DROP" "DESCRIBE"

# Repeat for silver and gold databases
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
    --resource '{"Table":{"DatabaseName":"mfg_silver_db","TableWildcard":{}}}' \
    --permissions "SELECT" "INSERT" "DELETE" "ALTER" "DROP" "DESCRIBE"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
    --resource '{"Table":{"DatabaseName":"mfg_gold_db","TableWildcard":{}}}' \
    --permissions "SELECT" "INSERT" "DELETE" "ALTER" "DROP" "DESCRIBE"
```

### Grant Analyst Read-Only Access to Gold Layer

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ANALYST_ARN="arn:aws:iam::${ACCOUNT_ID}:user/mfg-analyst"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ARN}" \
    --resource '{
        "Table": {
            "DatabaseName": "mfg_gold_db",
            "TableWildcard": {}
        }
    }' \
    --permissions "SELECT" "DESCRIBE"
```

### Grant Read Access to Specific Tables Only

```bash
# Analyst can only query the operation summary, not defect details
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ARN}" \
    --resource '{
        "Table": {
            "DatabaseName": "mfg_gold_db",
            "Name": "gold_operation_summary"
        }
    }' \
    --permissions "SELECT" "DESCRIBE"
```

---

## 8. Creating Users with Different Access Levels

### Role Definitions

| Role | Access Level | Databases | Permissions |
|------|-------------|-----------|-------------|
| **Data Engineer** | Full access | All layers | SELECT, INSERT, DELETE, ALTER, DROP |
| **Data Analyst** | Read Gold | `mfg_gold_db` | SELECT, DESCRIBE |
| **Data Scientist** | Read Silver + Gold | `mfg_silver_db`, `mfg_gold_db` | SELECT, DESCRIBE |
| **ETL Service** | Write all | All layers | SELECT, INSERT, DELETE, ALTER, DROP |
| **Viewer** | Read Gold summary only | `mfg_gold_db` specific tables | SELECT |

### Creating Each User Type

#### Data Engineer User

```bash
# IAM user
aws iam create-user --user-name mfg-data-engineer
aws iam attach-user-policy --user-name mfg-data-engineer \
    --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
aws iam attach-user-policy --user-name mfg-data-engineer \
    --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam attach-user-policy --user-name mfg-data-engineer \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

# Lake Formation: full access to all databases
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
DE_ARN="arn:aws:iam::${ACCOUNT_ID}:user/mfg-data-engineer"

for DB in mfg_bronze_db mfg_silver_db mfg_gold_db; do
    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${DE_ARN}" \
        --resource "{\"Database\":{\"Name\":\"${DB}\"}}" \
        --permissions "DESCRIBE"

    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${DE_ARN}" \
        --resource "{\"Table\":{\"DatabaseName\":\"${DB}\",\"TableWildcard\":{}}}" \
        --permissions "SELECT" "INSERT" "DELETE" "ALTER" "DROP" "DESCRIBE"
done
```

#### Analyst User (Gold Only)

```bash
aws iam create-user --user-name mfg-analyst
aws iam attach-user-policy --user-name mfg-analyst \
    --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
# Athena needs S3 access to write results to the results bucket
# Create a scoped S3 policy allowing only athena-results/
# (not shown for brevity)

ANALYST_ARN="arn:aws:iam::${ACCOUNT_ID}:user/mfg-analyst"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ARN}" \
    --resource '{"Database":{"Name":"mfg_gold_db"}}' \
    --permissions "DESCRIBE"

aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ARN}" \
    --resource '{"Table":{"DatabaseName":"mfg_gold_db","TableWildcard":{}}}' \
    --permissions "SELECT" "DESCRIBE"
```

#### Data Scientist User (Silver + Gold, No Bronze)

```bash
aws iam create-user --user-name mfg-data-scientist
# Attach Athena + S3 scoped policies...

DS_ARN="arn:aws:iam::${ACCOUNT_ID}:user/mfg-data-scientist"

for DB in mfg_silver_db mfg_gold_db; do
    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${DS_ARN}" \
        --resource "{\"Database\":{\"Name\":\"${DB}\"}}" \
        --permissions "DESCRIBE"

    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${DS_ARN}" \
        --resource "{\"Table\":{\"DatabaseName\":\"${DB}\",\"TableWildcard\":{}}}" \
        --permissions "SELECT" "DESCRIBE"
done
```

### Column-Level Security Example

If `silver_operator` contained a PII column (e.g., `personal_email`), restrict analysts from seeing it:

```bash
# Grant SELECT but exclude a sensitive column
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${ANALYST_ARN}" \
    --resource '{
        "TableWithColumns": {
            "DatabaseName": "mfg_silver_db",
            "Name": "silver_operator",
            "ColumnWildcard": {
                "ExcludedColumnNames": ["hire_date", "contact_email"]
            }
        }
    }' \
    --permissions "SELECT" "DESCRIBE"
```

---

## 9. Lake Formation + Iceberg: Important Notes

### DROP TABLE Requires Lake Formation DROP Permission

When running `DROP TABLE IF EXISTS` (e.g., in the cleanup job), the executing principal
must have **Lake Formation DROP** permission on the table — S3 delete permission alone
is not enough.

This is why the cleanup operation (`00_cleanup_tables.py`) runs as an AWS Glue job using
`GlueServiceRole-MfgLakehouse` (which has DROP permission), rather than being run via
the AWS CLI with a developer user that may lack LF DROP.

```python
# In 00_cleanup_tables.py — runs as Glue role with LF DROP permission
spark.sql(f"DROP TABLE IF EXISTS {table}")
```

If you get `AccessDeniedException: Required Drop on <table>`, check:
1. Does the executing role have LF DROP permission?
2. Is the role listed as a data lake admin?

### Iceberg Catalog Updates Need ALTER Permission

When an Iceberg job commits a new snapshot, it calls `glue:UpdateTable` to update the
`metadata_location` parameter. This requires:
- Lake Formation `ALTER` permission on the table (LF)
- `glue:UpdateTable` permission (IAM)

Both are covered by `AWSGlueServiceRole` (managed policy) + the LF grants above.

---

## 10. Permission Hierarchy Diagram

```
                    ┌─────────────────────────────────────────┐
                    │           AWS Account Root               │
                    │  (ultimate admin, avoid direct use)      │
                    └──────────────┬──────────────────────────┘
                                   │ sets Lake Formation admins
                    ┌──────────────▼──────────────────────────┐
                    │         Lake Formation Admins            │
                    │   mfg-dev-user, GlueServiceRole          │
                    │   (can grant/revoke any LF permission)   │
                    └──────┬──────────────┬────────────────────┘
                           │              │
          ┌────────────────▼──┐   ┌───────▼──────────────────────┐
          │  GlueServiceRole  │   │    Developer / Other Users   │
          │  (ETL jobs)        │   │                              │
          │  LF: ALL on all   │   │  LF: scoped per role         │
          │  IAM: S3+Glue     │   │  IAM: Athena+Glue read       │
          └───────────────────┘   └──────────────────────────────┘
                                          │
                      ┌───────────────────┼───────────────┐
                      │                   │               │
               ┌──────▼────┐    ┌─────────▼─────┐  ┌──────▼────┐
               │ Analyst    │    │ Data Scientist │  │  Viewer   │
               │ Gold only  │    │ Silver + Gold  │  │ Summary   │
               └────────────┘    └───────────────┘  └───────────┘
```

---

## 11. Troubleshooting Common Errors

### "AccessDeniedException: Required Drop on <table>"

**Cause:** The principal running DROP TABLE does not have Lake Formation DROP permission.
**Fix:** Add DROP to the principal's LF permissions, or run the operation as a Glue job
using the Glue role (which has full LF admin permissions).

### "AccessDeniedException: Insufficient Lake Formation permission"

**Cause:** The user has IAM permissions but Lake Formation has no explicit grant.
**Fix:** Grant the appropriate LF permission (`SELECT`, `INSERT`, etc.) to the principal.

### "EntityNotFoundException" when querying a table

**Cause:** The user has LF permission to `SELECT` but not `DESCRIBE` — can't see the schema.
**Fix:** Always grant `DESCRIBE` alongside `SELECT`.

### "The Glue catalog table was created with an incompatible LOCATION"

**Cause:** A previous version of the table was created pointing to a different S3 bucket.
`CREATE TABLE IF NOT EXISTS` skips creation because the table already exists, but it
points to the old location.
**Fix:** Run `00_cleanup_tables.py` as a Glue job to DROP all tables, then re-run the pipeline.
The Glue role has LF DROP permission and can remove them.
