#!/usr/bin/env bash
# =============================================================================
# 1_upload_raw_data.sh
# Uploads seed data and Glue scripts to S3.
# Run from project root: C:/DataEngineer/iceberg_medalion/
# =============================================================================
set -e

# ─── Configuration ────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-${AWS_REGION}"

echo "============================================"
echo "  Manufacturing Lakehouse - S3 Upload"
echo "  Bucket: s3://${BUCKET_NAME}"
echo "  Region: ${AWS_REGION}"
echo "============================================"

# ─── Upload data_1 (initial load) ─────────────────────────────────────────────
echo ""
echo "[1/4] Uploading data_1 files..."

aws s3 cp data/data_1/operator.csv \
    "s3://${BUCKET_NAME}/raw/data_1/operator/operator.csv"
echo "  ✓ operator.csv"

aws s3 cp data/data_1/machine.csv \
    "s3://${BUCKET_NAME}/raw/data_1/machine/machine.csv"
echo "  ✓ machine.csv"

aws s3 cp data/data_1/material.csv \
    "s3://${BUCKET_NAME}/raw/data_1/material/material.csv"
echo "  ✓ material.csv"

aws s3 cp data/data_1/supplier.json \
    "s3://${BUCKET_NAME}/raw/data_1/supplier/supplier.json"
echo "  ✓ supplier.json"

aws s3 cp data/data_1/operation_log.json \
    "s3://${BUCKET_NAME}/raw/data_1/operation_log/operation_log.json"
echo "  ✓ operation_log.json"

# ─── Upload data_2 (incremental load) ─────────────────────────────────────────
echo ""
echo "[2/4] Uploading data_2 files..."

aws s3 cp data/data_2/operator.csv \
    "s3://${BUCKET_NAME}/raw/data_2/operator/operator.csv"
echo "  ✓ operator.csv"

aws s3 cp data/data_2/machine.csv \
    "s3://${BUCKET_NAME}/raw/data_2/machine/machine.csv"
echo "  ✓ machine.csv"

aws s3 cp data/data_2/material.csv \
    "s3://${BUCKET_NAME}/raw/data_2/material/material.csv"
echo "  ✓ material.csv"

aws s3 cp data/data_2/supplier.json \
    "s3://${BUCKET_NAME}/raw/data_2/supplier/supplier.json"
echo "  ✓ supplier.json"

aws s3 cp data/data_2/operation_log.json \
    "s3://${BUCKET_NAME}/raw/data_2/operation_log/operation_log.json"
echo "  ✓ operation_log.json"

# ─── Upload Glue scripts ───────────────────────────────────────────────────────
echo ""
echo "[3/4] Uploading Glue ETL scripts..."

for script in glue_jobs/*.py; do
    filename=$(basename "${script}")
    aws s3 cp "${script}" "s3://${BUCKET_NAME}/glue-scripts/${filename}"
    echo "  ✓ ${filename}"
done

# ─── Verify uploads ────────────────────────────────────────────────────────────
echo ""
echo "[4/4] Verifying uploads..."
echo ""
echo "--- raw/data_1/ ---"
aws s3 ls "s3://${BUCKET_NAME}/raw/data_1/" --recursive \
    --human-readable | awk '{print "  " $1, $2, $3, $4, $5}'

echo ""
echo "--- raw/data_2/ ---"
aws s3 ls "s3://${BUCKET_NAME}/raw/data_2/" --recursive \
    --human-readable | awk '{print "  " $1, $2, $3, $4, $5}'

echo ""
echo "--- glue-scripts/ ---"
aws s3 ls "s3://${BUCKET_NAME}/glue-scripts/" \
    --human-readable | awk '{print "  " $1, $2, $3, $4, $5}'

echo ""
echo "============================================"
echo "  Upload complete!"
echo "  Bucket: s3://${BUCKET_NAME}"
echo "============================================"
