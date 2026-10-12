#!/usr/bin/env bash
# =============================================================================
# 3_sync_glue_jobs.sh
# Syncs glue_jobs/ → S3 glue-scripts/ and updates each Glue job's ScriptLocation.
# Run from project root after any change to a Glue script.
# =============================================================================
set -e

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-${AWS_REGION}"

echo "============================================"
echo "  Sync Glue Scripts → S3"
echo "  Bucket: s3://${BUCKET_NAME}/glue-scripts/"
echo "============================================"

# ─── Upload all Glue job scripts ──────────────────────────────────────────────
echo ""
echo "[1/2] Uploading scripts..."

for script in glue_jobs/*.py; do
    filename=$(basename "${script}")
    aws s3 cp "${script}" "s3://${BUCKET_NAME}/glue-scripts/${filename}"
    echo "  ✓ ${filename}"
done

# ─── Update ScriptLocation on each Glue job (picks up new code immediately) ───
echo ""
echo "[2/2] Updating Glue job ScriptLocation references..."

declare -A JOB_SCRIPTS=(
    ["mfg-raw-to-bronze-operator"]="01_raw_to_bronze_operator.py"
    ["mfg-raw-to-bronze-machine"]="02_raw_to_bronze_machine.py"
    ["mfg-raw-to-bronze-material"]="03_raw_to_bronze_material.py"
    ["mfg-raw-to-bronze-supplier"]="04_raw_to_bronze_supplier.py"
    ["mfg-raw-to-bronze-oplog"]="05_raw_to_bronze_operation_log.py"
    ["mfg-bronze-to-silver"]="06_bronze_to_silver.py"
    ["mfg-silver-to-gold"]="07_silver_to_gold.py"
)

for job_name in "${!JOB_SCRIPTS[@]}"; do
    script_file="${JOB_SCRIPTS[$job_name]}"
    aws glue update-job \
        --region "${AWS_REGION}" \
        --job-name "${job_name}" \
        --job-update "Command={Name=glueetl,ScriptLocation=s3://${BUCKET_NAME}/glue-scripts/${script_file},PythonVersion=3}" \
        --output text --query 'JobName' 2>/dev/null \
    && echo "  ✓ ${job_name} → ${script_file}" \
    || echo "  ! ${job_name} not found (run 2_create_glue_catalog.sh first)"
done

echo ""
echo "============================================"
echo "  Sync complete. Scripts are live in AWS."
echo "============================================"
