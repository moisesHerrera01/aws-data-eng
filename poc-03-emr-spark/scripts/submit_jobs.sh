#!/bin/bash
# POC 03 - Submit EMR Serverless Spark jobs
# Orchestrates: generate data -> Bronze->Silver -> Silver->Gold
set -e

STACK_NAME="poc03-emr-spark"
REGION="us-east-1"
PYTHON="/c/Users/moise/AppData/Local/Programs/Python/Python312/python.exe"

# Read outputs from CloudFormation
BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text)
APP_ID=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='EmrAppId'].OutputValue" --output text)
ROLE_ARN=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='EmrRoleArn'].OutputValue" --output text)

echo ">>> Bucket  : $BUCKET"
echo ">>> EMR App : $APP_ID"
echo ">>> Role    : $ROLE_ARN"

# ---- Step 1: Generate Bronze data ----
echo ""
echo ">>> [1/3] Generating Bronze data..."
$PYTHON scripts/generate_data.py "$BUCKET"

# ---- Step 2: Upload PySpark scripts to S3 ----
echo ""
echo ">>> [2/3] Uploading Spark scripts to S3..."
aws s3 cp spark/bronze_to_silver.py "s3://${BUCKET}/scripts/bronze_to_silver.py"
aws s3 cp spark/silver_to_gold.py   "s3://${BUCKET}/scripts/silver_to_gold.py"

# ---- Helper: submit job and wait ----
submit_and_wait() {
  local JOB_NAME=$1
  local SCRIPT=$2

  echo ""
  echo ">>> Submitting job: $JOB_NAME"
  JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id "$APP_ID" \
    --execution-role-arn "$ROLE_ARN" \
    --region "$REGION" \
    --name "$JOB_NAME" \
    --job-driver "{
      \"sparkSubmit\": {
        \"entryPoint\": \"s3://${BUCKET}/scripts/${SCRIPT}\",
        \"entryPointArguments\": [\"${BUCKET}\"],
        \"sparkSubmitParameters\": \"--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.executor.instances=1\"
      }
    }" \
    --configuration-overrides "{
      \"monitoringConfiguration\": {
        \"s3MonitoringConfiguration\": {
          \"logUri\": \"s3://${BUCKET}/logs/\"
        }
      }
    }" \
    --query "jobRunId" --output text)

  echo "    Job run ID: $JOB_RUN_ID"
  echo "    Waiting for completion..."

  while true; do
    STATUS=$(aws emr-serverless get-job-run \
      --application-id "$APP_ID" \
      --job-run-id "$JOB_RUN_ID" \
      --region "$REGION" \
      --query "jobRun.state" --output text)

    echo "    Status: $STATUS"

    case $STATUS in
      SUCCESS)
        echo "    Job $JOB_NAME completed successfully."
        break
        ;;
      FAILED|CANCELLED)
        echo "    ERROR: Job $JOB_NAME $STATUS"
        aws emr-serverless get-job-run \
          --application-id "$APP_ID" \
          --job-run-id "$JOB_RUN_ID" \
          --region "$REGION" \
          --query "jobRun.stateDetails" --output text
        exit 1
        ;;
      *)
        sleep 15
        ;;
    esac
  done
}

# ---- Step 3: Run Spark jobs in sequence ----
submit_and_wait "bronze-to-silver" "bronze_to_silver.py"
submit_and_wait "silver-to-gold"   "silver_to_gold.py"

# ---- Step 4: Show results ----
echo ""
echo ">>> [3/3] Pipeline complete. Gold layer in S3:"
aws s3 ls "s3://${BUCKET}/gold/" --recursive --human-readable --region "$REGION"
