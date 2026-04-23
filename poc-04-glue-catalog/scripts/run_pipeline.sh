#!/bin/bash
# POC 04 - Run full pipeline
# Usage:
#   bash scripts/run_pipeline.sh          # runs batch1 (initial load)
#   bash scripts/run_pipeline.sh batch2   # runs batch2 (incremental)
set -e

STACK_NAME="poc04-glue-catalog"
REGION="us-east-1"
PYTHON="/c/Users/moise/AppData/Local/Programs/Python/Python312/python.exe"
BATCH="${1:-batch1}"

# Read CFN outputs
BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='DataLakeBucket'].OutputValue" --output text)
JOB_NAME=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='GlueJobName'].OutputValue" --output text)
WORKGROUP=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='AthenaWorkgroup'].OutputValue" --output text)
ATHENA_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='AthenaResultsBucket'].OutputValue" --output text)

echo ">>> Bucket   : $BUCKET"
echo ">>> Glue Job : $JOB_NAME"
echo ">>> Batch    : $BATCH"

# ---- Step 1: Upload Glue script ----
echo ""
echo ">>> [1] Uploading Glue ETL script..."
aws s3 cp glue/etl_job.py "s3://${BUCKET}/scripts/etl_job.py"

# ---- Step 2: Generate Bronze data ----
echo ""
echo ">>> [2] Generating Bronze data ($BATCH)..."
$PYTHON scripts/generate_data.py "$BUCKET" "$BATCH"

# ---- Step 3: Run Bronze crawler ----
echo ""
echo ">>> [3] Running Bronze crawler..."
aws glue start-crawler --name poc04-crawler-bronze --region "$REGION"
echo "    Waiting for crawler..."
while true; do
  STATE=$(aws glue get-crawler --name poc04-crawler-bronze --region "$REGION" \
    --query "Crawler.State" --output text)
  [ "$STATE" = "READY" ] && break
  echo "    State: $STATE"
  sleep 10
done
echo "    Crawler complete."

# ---- Step 4: Run Glue ETL Job (with bookmark) ----
echo ""
echo ">>> [4] Starting Glue job: $JOB_NAME..."
RUN_ID=$(aws glue start-job-run \
  --job-name "$JOB_NAME" \
  --region "$REGION" \
  --arguments "{\"--BUCKET\":\"${BUCKET}\"}" \
  --query "JobRunId" --output text)

echo "    Job Run ID: $RUN_ID"
echo "    Waiting for completion..."
while true; do
  STATUS=$(aws glue get-job-run \
    --job-name "$JOB_NAME" \
    --run-id "$RUN_ID" \
    --region "$REGION" \
    --query "JobRun.JobRunState" --output text)
  echo "    Status: $STATUS"
  case $STATUS in
    SUCCEEDED) echo "    Job completed."; break ;;
    FAILED|ERROR|TIMEOUT)
      aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
        --region "$REGION" --query "JobRun.ErrorMessage" --output text
      exit 1 ;;
    *) sleep 15 ;;
  esac
done

# ---- Step 5: Run Silver crawler ----
echo ""
echo ">>> [5] Running Silver crawler..."
aws glue start-crawler --name poc04-crawler-silver --region "$REGION"
while true; do
  STATE=$(aws glue get-crawler --name poc04-crawler-silver --region "$REGION" \
    --query "Crawler.State" --output text)
  [ "$STATE" = "READY" ] && break
  echo "    State: $STATE"
  sleep 10
done
echo "    Crawler complete."

# ---- Step 6: Show catalog tables ----
echo ""
echo ">>> [6] Glue Catalog tables:"
echo "  [bronze]"
aws glue get-tables --database-name bronze --region "$REGION" \
  --query "TableList[*].Name" --output table
echo "  [silver]"
aws glue get-tables --database-name silver --region "$REGION" \
  --query "TableList[*].Name" --output table

# ---- Step 7: Athena query on Silver ----
echo ""
echo ">>> [7] Athena query — orders count per status from Silver..."
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT status, COUNT(*) as total, ROUND(SUM(amount),2) as revenue FROM silver.orders GROUP BY status ORDER BY total DESC;" \
  --work-group "$WORKGROUP" \
  --region "$REGION" \
  --query "QueryExecutionId" --output text)

echo "    Query ID: $QUERY_ID"
while true; do
  STATE=$(aws athena get-query-execution --query-execution-id "$QUERY_ID" \
    --region "$REGION" --query "QueryExecution.Status.State" --output text)
  [ "$STATE" = "SUCCEEDED" ] && break
  [ "$STATE" = "FAILED" ] && echo "Athena query failed" && exit 1
  sleep 5
done

echo ""
aws athena get-query-results --query-execution-id "$QUERY_ID" \
  --region "$REGION" \
  --query "ResultSet.Rows[*].Data[*].VarCharValue" \
  --output table

echo ""
echo ">>> Pipeline complete for $BATCH."
[ "$BATCH" = "batch1" ] && echo "    Run again with: bash scripts/run_pipeline.sh batch2 (to test Job Bookmark)"
