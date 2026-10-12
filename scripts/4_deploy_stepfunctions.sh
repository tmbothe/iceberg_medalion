#!/usr/bin/env bash
# =============================================================================
# 4_deploy_stepfunctions.sh
# Creates or updates the Step Functions state machine from the local definition.
# Run from project root after any change to infrastructure/stepfunctions/pipeline_definition.json
# =============================================================================
set -e

AWS_REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
STATE_MACHINE_NAME="Manufacturing_Lakehouse_ETL_Pipeline"
STATE_MACHINE_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}"
DEFINITION_FILE="infrastructure/stepfunctions/pipeline_definition.json"
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-MfgLakehouse"

echo "============================================"
echo "  Deploy Step Functions State Machine"
echo "  Name:   ${STATE_MACHINE_NAME}"
echo "  Region: ${AWS_REGION}"
echo "============================================"

if [ ! -f "${DEFINITION_FILE}" ]; then
    echo "ERROR: Definition file not found: ${DEFINITION_FILE}"
    echo "Run from project root: bash scripts/4_deploy_stepfunctions.sh"
    exit 1
fi

DEFINITION=$(cat "${DEFINITION_FILE}")

# Try update first, create if it doesn't exist
if aws stepfunctions describe-state-machine \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --region "${AWS_REGION}" \
        --output text --query 'name' &>/dev/null; then

    echo ""
    echo "Updating existing state machine..."
    aws stepfunctions update-state-machine \
        --state-machine-arn "${STATE_MACHINE_ARN}" \
        --definition "${DEFINITION}" \
        --region "${AWS_REGION}" \
        --query 'updateDate' --output text
    echo "  ✓ State machine updated"

else
    echo ""
    echo "Creating new state machine..."
    aws stepfunctions create-state-machine \
        --name "${STATE_MACHINE_NAME}" \
        --definition "${DEFINITION}" \
        --role-arn "${GLUE_ROLE_ARN}" \
        --type STANDARD \
        --region "${AWS_REGION}" \
        --query 'stateMachineArn' --output text
    echo "  ✓ State machine created"
fi

echo ""
echo "============================================"
echo "  State machine is live. Run pipeline with:"
echo ""
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn ${STATE_MACHINE_ARN} \\"
echo "    --input '{}'"
echo "  (uses all defaults: incremental, data_2)"
echo "============================================"
