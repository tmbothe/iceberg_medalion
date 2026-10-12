# AWS Prerequisites & Account Setup

## Table of Contents
1. [AWS Account Requirements](#1-aws-account-requirements)
2. [AWS CLI Setup](#2-aws-cli-setup)
3. [S3 Bucket](#3-s3-bucket)
4. [IAM Role for Glue](#4-iam-role-for-glue)
5. [IAM User for Development](#5-iam-user-for-development)
6. [Required IAM Permissions Summary](#6-required-iam-permissions-summary)
7. [Verify Your Setup](#7-verify-your-setup)

---

## 1. AWS Account Requirements

You need an AWS account with the following services enabled (all are available by default):

| Service | Purpose |
|---------|---------|
| Amazon S3 | Store raw data, Iceberg files, scripts |
| AWS Glue | ETL jobs (PySpark), Data Catalog |
| Amazon Athena | Ad-hoc SQL queries on Iceberg tables |
| AWS Step Functions | Pipeline orchestration |
| AWS Lake Formation | Data governance and access control |
| AWS IAM | Identity and access management |

**Estimated cost for this project** (small dataset, occasional runs):
- S3: < $0.01/month
- Glue: ~$0.44 per DPU-hour × 2 workers × ~5 minutes per job = ~$0.07 per pipeline run
- Athena: $5 per TB scanned (negligible for small tables)
- Step Functions: Free tier covers first 4,000 state transitions/month

---

## 2. AWS CLI Setup

### Install AWS CLI v2

**macOS:**
```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
```

**Linux:**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install
```

**Windows:**
Download and run the MSI installer from the AWS documentation.

Verify: `aws --version`

### Configure AWS CLI

```bash
aws configure
# AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
# AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
# Default region name [None]: us-east-1
# Default output format [None]: json
```

This creates `~/.aws/credentials` and `~/.aws/config`.

### Multiple Profiles (Optional)

If you manage multiple AWS accounts:
```bash
aws configure --profile mfg-lakehouse
# Then use: aws s3 ls --profile mfg-lakehouse
# Or: export AWS_PROFILE=mfg-lakehouse
```

### Verify Identity
```bash
aws sts get-caller-identity
# {
#   "UserId": "AIDAIOSFODNN7EXAMPLE",
#   "Account": "442042510197",
#   "Arn": "arn:aws:iam::442042510197:user/your-username"
# }
```

---

## 3. S3 Bucket

The bucket name follows the convention: `mfg-lakehouse-<account_id>-<region>`

This ensures global uniqueness (S3 bucket names are globally unique) and embeds the
account/region for easy identification.

### Create the Bucket

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-east-1"
BUCKET_NAME="mfg-lakehouse-${ACCOUNT_ID}-${REGION}"

aws s3api create-bucket \
    --bucket "${BUCKET_NAME}" \
    --region "${REGION}"
```

> Note: For `us-east-1`, do NOT specify `--create-bucket-configuration`. For all other
> regions, add: `--create-bucket-configuration LocationConstraint=${REGION}`

### Enable Versioning (Recommended)

```bash
aws s3api put-bucket-versioning \
    --bucket "${BUCKET_NAME}" \
    --versioning-configuration Status=Enabled
```

### Block Public Access

```bash
aws s3api put-public-access-block \
    --bucket "${BUCKET_NAME}" \
    --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

### Expected S3 Folder Structure

After running the pipeline, the bucket will look like:

```
s3://mfg-lakehouse-442042510197-us-east-1/
├── raw/
│   ├── data_1/
│   │   ├── operator/operator.csv
│   │   ├── machine/machine.csv
│   │   ├── material/material.csv
│   │   ├── supplier/supplier.json
│   │   └── operation_log/operation_log.json
│   └── data_2/
│       └── (same structure)
├── bronze/
│   ├── operator/       ← Iceberg data + metadata
│   ├── machine/
│   ├── material/
│   ├── supplier/
│   └── operation_log/
├── silver/
│   ├── operator/
│   ├── machine/
│   ├── material/
│   ├── supplier/
│   └── operation_log/
├── gold/
│   ├── operation_summary/
│   ├── defect_analysis/
│   └── supplier_performance/
├── glue-scripts/           ← Glue ETL Python scripts
├── infrastructure/         ← Step Functions definition (archival)
└── athena-results/         ← Athena query result files
```

---

## 4. IAM Role for Glue

AWS Glue jobs run as an IAM role. This role needs permissions to read/write S3,
manage Glue catalog entries, and interact with Lake Formation.

### Create the Role

```bash
# Create trust policy for Glue service
cat > /tmp/glue-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "glue.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
    --role-name GlueServiceRole-MfgLakehouse \
    --assume-role-policy-document file:///tmp/glue-trust-policy.json \
    --description "Glue ETL role for Manufacturing Lakehouse project"
```

### Attach AWS Managed Policies

```bash
# Core Glue permissions
aws iam attach-role-policy \
    --role-name GlueServiceRole-MfgLakehouse \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# S3 full access (or create a scoped custom policy — see below)
aws iam attach-role-policy \
    --role-name GlueServiceRole-MfgLakehouse \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### Attach Lake Formation Inline Policy

The inline policy in `infrastructure/iam/lf-access.json` allows the Glue role to get
data access from Lake Formation (required for Iceberg catalog operations):

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws iam put-role-policy \
    --role-name GlueServiceRole-MfgLakehouse \
    --policy-name LakeFormationDataAccess \
    --policy-document file://infrastructure/iam/lf-access.json
```

### Scoped S3 Custom Policy (More Secure Alternative)

Instead of `AmazonS3FullAccess`, create a scoped policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
        "s3:GetBucketLocation", "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::mfg-lakehouse-442042510197-us-east-1",
        "arn:aws:s3:::mfg-lakehouse-442042510197-us-east-1/*"
      ]
    }
  ]
}
```

---

## 5. IAM User for Development

This is the IAM user you use locally (via AWS CLI) to deploy scripts, run queries, etc.

### Create a Development User

```bash
aws iam create-user --user-name mfg-dev-user

# Create access key for CLI
aws iam create-access-key --user-name mfg-dev-user
# Save the AccessKeyId and SecretAccessKey — shown only once
```

### Attach Required Policies to Dev User

```bash
# S3 access
aws iam attach-user-policy \
    --user-name mfg-dev-user \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Glue: manage jobs, catalog
aws iam attach-user-policy \
    --user-name mfg-dev-user \
    --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess

# Step Functions
aws iam attach-user-policy \
    --user-name mfg-dev-user \
    --policy-arn arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess

# Athena
aws iam attach-user-policy \
    --user-name mfg-dev-user \
    --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess

# Lake Formation console access
aws iam attach-user-policy \
    --user-name mfg-dev-user \
    --policy-arn arn:aws:iam::aws:policy/AWSLakeFormationCrossAccountManager
```

### Lake Formation Admin for Dev User

To manage Lake Formation permissions, your dev user needs to be a Lake Formation admin:

1. Go to **AWS Console → Lake Formation → Administration → Data lake administrators**
2. Click **Add** → select `mfg-dev-user`
3. Click **Save**

Or via CLI:
```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws lakeformation put-data-lake-settings \
    --data-lake-settings '{
        "DataLakeAdmins": [
            {"DataLakePrincipalIdentifier": "arn:aws:iam::'"${ACCOUNT_ID}"':user/mfg-dev-user"}
        ]
    }'
```

> **Important:** When Lake Formation is first enabled, no IAM principal (other than the root
> account or Lake Formation admins) can access the Glue catalog tables. You must explicitly
> grant permissions. See [06_lake_formation.md](06_lake_formation.md) for full details.

---

## 6. Required IAM Permissions Summary

| Principal | Needs access to | Why |
|-----------|----------------|-----|
| `GlueServiceRole-MfgLakehouse` | S3 read/write on bucket | Read raw data, write Iceberg files |
| `GlueServiceRole-MfgLakehouse` | Glue catalog: CreateTable, UpdateTable, DeleteTable, GetTable | Iceberg catalog commits |
| `GlueServiceRole-MfgLakehouse` | Lake Formation: GetDataAccess | Table-level governance |
| `mfg-dev-user` | S3, Glue, Step Functions, Athena | Deploy and run pipeline |
| `mfg-dev-user` | Lake Formation admin | Grant permissions to other principals |
| `bedrock-user` (or analyst) | Lake Formation SELECT on specific tables | Query Gold layer via Athena |

---

## 7. Verify Your Setup

Run these checks before deploying:

```bash
# 1. Verify AWS CLI identity
aws sts get-caller-identity

# 2. Verify S3 bucket exists and is accessible
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
aws s3 ls s3://mfg-lakehouse-${ACCOUNT_ID}-us-east-1/

# 3. Verify Glue role exists
aws iam get-role --role-name GlueServiceRole-MfgLakehouse --query Role.RoleName

# 4. Verify Lake Formation settings
aws lakeformation get-data-lake-settings --query DataLakeSettings.DataLakeAdmins

# 5. Verify Glue databases exist (after running create_glue_catalog.sh)
aws glue get-databases --query 'DatabaseList[?contains(Name, `mfg`)].Name'
```

Once all checks pass, proceed to [13_deployment_guide.md](13_deployment_guide.md) for the
full step-by-step deployment.
