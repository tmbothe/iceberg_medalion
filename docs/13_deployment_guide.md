# Deployment Guide — From Zero to Running Pipeline

## Table of Contents
1. [Prerequisites Checklist](#1-prerequisites-checklist)
2. [Step 1: Clone the Repository](#2-step-1-clone-the-repository)
3. [Step 2: Create S3 Bucket](#3-step-2-create-s3-bucket)
4. [Step 3: Create IAM Role for Glue](#4-step-3-create-iam-role-for-glue)
5. [Step 4: Configure Lake Formation](#5-step-4-configure-lake-formation)
6. [Step 5: Upload Data and Scripts to S3](#6-step-5-upload-data-and-scripts-to-s3)
7. [Step 6: Create Glue Databases and Jobs](#7-step-6-create-glue-databases-and-jobs)
8. [Step 7: Deploy Step Functions](#8-step-7-deploy-step-functions)
9. [Step 8: Run Initial Load (data_1)](#9-step-8-run-initial-load-data_1)
10. [Step 9: Validate Initial Load](#10-step-9-validate-initial-load)
11. [Step 10: Run Incremental Load (data_2)](#11-step-10-run-incremental-load-data_2)
12. [Step 11: Validate Incremental Load](#12-step-11-validate-incremental-load)
13. [Re-deploying After Script Changes](#13-re-deploying-after-script-changes)
14. [Resetting Everything (Clean Slate)](#14-resetting-everything-clean-slate)

---

## 1. Prerequisites Checklist

Before starting, verify:

- [ ] AWS account available
- [ ] AWS CLI v2 installed and configured (`aws sts get-caller-identity` works)
- [ ] Git installed
- [ ] Bash shell available (Git Bash on Windows, Terminal on macOS/Linux)
- [ ] Python 3.x installed (for local data inspection only — Glue runs in AWS)
- [ ] Sufficient IAM permissions (see [05_aws_prerequisites.md](05_aws_prerequisites.md))

---

## 2. Step 1: Clone the Repository

```bash
git clone https://github.com/<your-org>/iceberg_medalion.git
cd iceberg_medalion
```

Verify the structure:
```bash
ls -la
# Should see: README.md, data/, docs/, glue_jobs/, infrastructure/, scripts/
```

---

## 3. Step 2: Create S3 Bucket

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-east-1"
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-${REGION}"

echo "Creating bucket: ${BUCKET_NAME}"

# Create bucket (us-east-1 only — no LocationConstraint needed)
aws s3api create-bucket \
    --bucket "${BUCKET_NAME}" \
    --region "${REGION}"

# Block public access
aws s3api put-public-access-block \
    --bucket "${BUCKET_NAME}" \
    --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Verify
aws s3 ls s3://${BUCKET_NAME}/
echo "Bucket created: s3://${BUCKET_NAME}"
```

---

## 4. Step 3: Create IAM Role for Glue

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create trust policy
cat > /tmp/glue-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

# Create role
aws iam create-role \
    --role-name GlueServiceRole-MfgLakehouse \
    --assume-role-policy-document file:///tmp/glue-trust-policy.json

# Attach managed policies
aws iam attach-role-policy \
    --role-name GlueServiceRole-MfgLakehouse \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --role-name GlueServiceRole-MfgLakehouse \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Attach Lake Formation inline policy
aws iam put-role-policy \
    --role-name GlueServiceRole-MfgLakehouse \
    --policy-name LakeFormationDataAccess \
    --policy-document file://infrastructure/iam/lf-access.json

echo "IAM role created: GlueServiceRole-MfgLakehouse"
```

---

## 5. Step 4: Configure Lake Formation

### 4a. Add Lake Formation Admins

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CURRENT_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

aws lakeformation put-data-lake-settings \
    --data-lake-settings "{
        \"DataLakeAdmins\": [
            {\"DataLakePrincipalIdentifier\": \"${CURRENT_USER_ARN}\"},
            {\"DataLakePrincipalIdentifier\": \"${GLUE_ROLE_ARN}\"}
        ],
        \"CreateDatabaseDefaultPermissions\": [],
        \"CreateTableDefaultPermissions\": []
    }"
```

### 4b. Register the S3 Location

```bash
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-us-east-1"
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

aws lakeformation register-resource \
    --resource-arn "arn:aws:s3:::${BUCKET_NAME}" \
    --role-arn "${GLUE_ROLE_ARN}" \
    --use-service-linked-role false

echo "S3 location registered with Lake Formation"
```

> If you see `AlreadyExistsException`, the bucket is already registered — that's fine.

### 4c. Grant Database Permissions to Glue Role

Run this after Step 6 creates the databases, or run it now (it will fail silently on
non-existent databases and succeed after creation):

```bash
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

for DB in mfg_bronze_db mfg_silver_db mfg_gold_db; do
    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
        --resource "{\"Database\":{\"Name\":\"${DB}\"}}" \
        --permissions "CREATE_TABLE" "ALTER" "DROP" "DESCRIBE" 2>/dev/null

    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
        --resource "{\"Table\":{\"DatabaseName\":\"${DB}\",\"TableWildcard\":{}}}" \
        --permissions "SELECT" "INSERT" "DELETE" "ALTER" "DROP" "DESCRIBE" 2>/dev/null

    echo "  Granted permissions on ${DB}"
done
```

---

## 6. Step 5: Upload Data and Scripts to S3

```bash
# From project root:
bash scripts/1_upload_raw_data.sh
```

This uploads:
- `data/data_1/` → `s3://bucket/raw/data_1/`
- `data/data_2/` → `s3://bucket/raw/data_2/`
- `glue_jobs/*.py` → `s3://bucket/glue-scripts/`

Verify uploads:
```bash
BUCKET_NAME="mfg-lakehouse-$(aws sts get-caller-identity --query Account --output text)-us-east-1"
aws s3 ls s3://${BUCKET_NAME}/raw/data_1/ --recursive
aws s3 ls s3://${BUCKET_NAME}/glue-scripts/
```

---

## 7. Step 6: Create Glue Databases and Jobs

```bash
bash scripts/2_create_glue_catalog.sh
```

This creates:
- 3 Glue databases: `mfg_bronze_db`, `mfg_silver_db`, `mfg_gold_db`
- 7 Glue ETL jobs with correct S3 script locations and Iceberg configuration
- Athena workgroup output location

Verify:
```bash
aws glue get-databases --query 'DatabaseList[?contains(Name,`mfg`)].Name'
aws glue list-jobs --query 'JobNames[?contains(@,`mfg`)]'
```

Now grant Lake Formation permissions (if not done in Step 4c):
```bash
# Re-run the grant commands from step 4c — they are idempotent
```

---

## 8. Step 7: Deploy Step Functions

```bash
bash scripts/4_deploy_stepfunctions.sh
```

This creates (or updates) the `Manufacturing_Lakehouse_ETL_Pipeline` state machine.

Verify:
```bash
aws stepfunctions list-state-machines \
    --query 'stateMachines[?contains(name,`Manufacturing`)].{Name:name,Status:status}'
```

---

## 9. Step 8: Run Initial Load (data_1)

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
STATE_MACHINE_ARN="arn:aws:states:us-east-1:${ACCOUNT_ID}:stateMachine:Manufacturing_Lakehouse_ETL_Pipeline"

EXECUTION_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    --input '{"load_batch": "data_1", "load_type": "initial"}' \
    --query 'executionArn' \
    --output text)

echo "Execution started: ${EXECUTION_ARN}"
echo "Monitor at: https://console.aws.amazon.com/states/home?region=us-east-1#/executions/details/${EXECUTION_ARN}"
```

### Monitor Progress

```bash
# Check status every 30 seconds
watch -n 30 "aws stepfunctions describe-execution \
    --execution-arn '${EXECUTION_ARN}' \
    --query '{Status:status,Start:startDate,Stop:stopDate}'"

# Or one-shot:
aws stepfunctions describe-execution \
    --execution-arn "${EXECUTION_ARN}" \
    --query '{Status:status,Stop:stopDate}'
```

**Expected total runtime:** 15–25 minutes (Glue job startup overhead is ~3–5 min per job)

---

## 10. Step 9: Validate Initial Load

Run these Athena queries to confirm the initial load succeeded:

```sql
-- Bronze: 5 tables, correct row counts
SELECT 'bronze_operator'     as tbl, COUNT(*) FROM mfg_bronze_db.bronze_operator
UNION ALL SELECT 'bronze_machine',     COUNT(*) FROM mfg_bronze_db.bronze_machine
UNION ALL SELECT 'bronze_material',    COUNT(*) FROM mfg_bronze_db.bronze_material
UNION ALL SELECT 'bronze_supplier',    COUNT(*) FROM mfg_bronze_db.bronze_supplier
UNION ALL SELECT 'bronze_operation_log', COUNT(*) FROM mfg_bronze_db.bronze_operation_log;
-- Expected: 8, 8, 8, 8, 10
```

```sql
-- Silver: all rows current (no SCD2 history yet)
SELECT is_current, COUNT(*) FROM mfg_silver_db.silver_operator GROUP BY is_current;
-- Expected: TRUE: 8
```

```sql
-- Gold: reports exist
SELECT * FROM mfg_gold_db.gold_operation_summary ORDER BY report_date;
```

---

## 11. Step 10: Run Incremental Load (data_2)

```bash
EXECUTION_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn "${STATE_MACHINE_ARN}" \
    --input '{}' \
    --query 'executionArn' \
    --output text)

echo "Incremental execution: ${EXECUTION_ARN}"
```

Using `{}` triggers the default parameters: `load_batch=data_2`, `load_type=incremental`.

---

## 12. Step 11: Validate Incremental Load

```sql
-- SCD2 check: should have both current and historical rows
SELECT is_current, COUNT(*) FROM mfg_silver_db.silver_operator GROUP BY is_current;
-- Expected: FALSE: 3, TRUE: 10

-- Verify a changed record
SELECT operator_id, workstation_id, shift, effective_from, effective_to, is_current
FROM mfg_silver_db.silver_operator
WHERE operator_id = 'OP002'
ORDER BY effective_from;

-- Operation log: all 15 rows
SELECT COUNT(*) FROM mfg_silver_db.silver_operation_log;
-- Expected: 15

-- Gold: updated with data_2 operations
SELECT * FROM mfg_gold_db.gold_operation_summary ORDER BY report_date;
```

Full validation queries are in [14_validation_and_testing.md](14_validation_and_testing.md).

---

## 13. Re-deploying After Script Changes

When you edit a Glue script locally, sync it to S3 and update the Glue job:

```bash
bash scripts/3_sync_glue_jobs.sh
```

When you edit the Step Functions definition:
```bash
bash scripts/4_deploy_stepfunctions.sh
```

The updated scripts take effect on the **next** job run — Glue always fetches the script
from S3 at job start time.

---

## 14. Resetting Everything (Clean Slate)

If you need to start fresh (e.g., after a failed run that left partial data):

### Option A: Drop Only Iceberg Tables

```bash
# Create and run the cleanup Glue job
aws glue create-job \
    --name mfg-cleanup-tables \
    --role "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/GlueServiceRole-MfgLakehouse" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://mfg-lakehouse-442042510197-us-east-1/glue-scripts/00_cleanup_tables.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--datalake-formats": "iceberg",
        "--BUCKET_NAME": "mfg-lakehouse-442042510197-us-east-1"
    }' \
    --glue-version "5.0" \
    --worker-type G.1X \
    --number-of-workers 2 2>/dev/null || echo "Job already exists"

aws glue start-job-run \
    --job-name mfg-cleanup-tables \
    --arguments '{"--BUCKET_NAME":"mfg-lakehouse-442042510197-us-east-1"}'
```

Then re-run the pipeline from Step 8.

> **Why run cleanup as a Glue job?** The Glue role has Lake Formation DROP permission.
> Running `DROP TABLE` via the AWS CLI with a developer user may fail with
> `AccessDeniedException: Required Drop on <table>` if that user lacks LF DROP.

### Option B: Full Reset (delete and recreate S3 paths)

```bash
BUCKET="mfg-lakehouse-$(aws sts get-caller-identity --query Account --output text)-us-east-1"

# Delete only the Iceberg data layers (keep raw/ and glue-scripts/)
aws s3 rm s3://${BUCKET}/bronze/ --recursive
aws s3 rm s3://${BUCKET}/silver/ --recursive
aws s3 rm s3://${BUCKET}/gold/ --recursive

# Then run the Glue cleanup job to drop catalog entries
# (S3 files are gone but Glue catalog still has the table entries)
aws glue start-job-run --job-name mfg-cleanup-tables \
    --arguments '{"--BUCKET_NAME":"'${BUCKET}'"}'
```

After cleanup, re-run from Step 8 (initial load).
