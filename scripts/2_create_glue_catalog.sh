#!/usr/bin/env bash
# =============================================================================
# 2_create_glue_catalog.sh
# Creates Glue Data Catalog databases and Glue ETL jobs (Glue 5.1, Iceberg).
# Run AFTER 1_upload_raw_data.sh
# =============================================================================
set -e

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-${AWS_REGION}"
GLUE_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

echo "============================================"
echo "  Creating Glue Catalog & Jobs"
echo "  Account: ${ACCOUNT_ID}"
echo "  Region:  ${AWS_REGION}"
echo "============================================"

# ─── Create Glue Databases ─────────────────────────────────────────────────────
echo ""
echo "[1/3] Creating Glue databases..."

aws glue create-database \
    --region ${AWS_REGION} \
    --database-input '{
        "Name": "mfg_bronze_db",
        "Description": "Manufacturing Lakehouse - Bronze layer (raw ingestion)",
        "Parameters": {"layer": "bronze", "project": "mfg-lakehouse"}
    }' 2>/dev/null || echo "  mfg_bronze_db already exists"

aws glue create-database \
    --region ${AWS_REGION} \
    --database-input '{
        "Name": "mfg_silver_db",
        "Description": "Manufacturing Lakehouse - Silver layer (cleansed + SCD2)",
        "Parameters": {"layer": "silver", "project": "mfg-lakehouse"}
    }' 2>/dev/null || echo "  mfg_silver_db already exists"

aws glue create-database \
    --region ${AWS_REGION} \
    --database-input '{
        "Name": "mfg_gold_db",
        "Description": "Manufacturing Lakehouse - Gold layer (aggregated reports)",
        "Parameters": {"layer": "gold", "project": "mfg-lakehouse"}
    }' 2>/dev/null || echo "  mfg_gold_db already exists"

echo "  ✓ Databases: mfg_bronze_db, mfg_silver_db, mfg_gold_db"

# ─── Create Glue ETL Jobs ──────────────────────────────────────────────────────
# Glue 5.1 + --datalake-formats iceberg loads Iceberg extensions automatically.
# Catalog configs are set in each script via SparkConf() before SparkContext().
echo ""
echo "[2/3] Creating Glue ETL jobs..."

create_glue_job() {
    local JOB_NAME=$1
    local SCRIPT=$2
    echo "  Creating: ${JOB_NAME}"
    aws glue create-job \
        --region ${AWS_REGION} \
        --name "${JOB_NAME}" \
        --role "${GLUE_ROLE}" \
        --command '{
            "Name": "glueetl",
            "ScriptLocation": "s3://'"${BUCKET_NAME}"'/glue-scripts/'"${SCRIPT}"'",
            "PythonVersion": "3"
        }' \
        --default-arguments '{
            "--job-language":           "python",
            "--job-bookmark-option":    "job-bookmark-disable",
            "--datalake-formats":       "iceberg",
            "--TempDir":                "s3://'"${BUCKET_NAME}"'/glue-temp/",
            "--enable-metrics":         "true",
            "--enable-spark-ui":        "true",
            "--spark-event-logs-path":  "s3://'"${BUCKET_NAME}"'/spark-ui/",
            "--enable-glue-datacatalog":"true",
            "--BUCKET_NAME":            "'"${BUCKET_NAME}"'",
            "--LOAD_BATCH":             "data_1"
        }' \
        --glue-version "5.0" \
        --worker-type "G.1X" \
        --number-of-workers 2 \
        --timeout 30 \
        --max-retries 1 2>/dev/null || echo "    (already exists)"
}

create_glue_job "mfg-raw-to-bronze-operator"   "01_raw_to_bronze_operator.py"
create_glue_job "mfg-raw-to-bronze-machine"    "02_raw_to_bronze_machine.py"
create_glue_job "mfg-raw-to-bronze-material"   "03_raw_to_bronze_material.py"
create_glue_job "mfg-raw-to-bronze-supplier"   "04_raw_to_bronze_supplier.py"
create_glue_job "mfg-raw-to-bronze-oplog"      "05_raw_to_bronze_operation_log.py"
create_glue_job "mfg-bronze-to-silver"         "06_bronze_to_silver.py"
create_glue_job "mfg-silver-to-gold"           "07_silver_to_gold.py"

echo "  ✓ All 7 Glue jobs created"

# ─── Configure Athena ─────────────────────────────────────────────────────────
echo ""
echo "[3/3] Configuring Athena output location..."

aws athena update-work-group \
    --region ${AWS_REGION} \
    --work-group primary \
    --configuration-updates \
        ResultConfigurationUpdates="{OutputLocation=s3://${BUCKET_NAME}/athena-results/}" \
    2>/dev/null || echo "  (Athena workgroup update skipped)"

echo "  ✓ Athena results → s3://${BUCKET_NAME}/athena-results/"

echo ""
echo "============================================"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "  3. Sync Glue scripts:  bash scripts/3_sync_glue_jobs.sh"
echo "  4. Deploy pipeline:    bash scripts/4_deploy_stepfunctions.sh"
echo ""
echo "  Then trigger via Step Functions (see README.md for commands)."
echo "============================================"
