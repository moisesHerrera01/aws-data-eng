#!/bin/bash
# POC 04 - Destroy stack and clean S3
set -e

STACK_NAME="poc04-glue-catalog"
REGION="us-east-1"

for KEY in DataLakeBucket AthenaResultsBucket; do
  BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='${KEY}'].OutputValue" --output text 2>/dev/null || echo "")
  if [ -n "$BUCKET" ] && [ "$BUCKET" != "None" ]; then
    echo ">>> Emptying bucket: $BUCKET..."
    aws s3 rm "s3://$BUCKET" --recursive --region "$REGION" 2>/dev/null || true
  fi
done

echo ">>> Deleting stack: $STACK_NAME..."
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"

echo "    Waiting for deletion..."
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"

echo ">>> Done. All POC 04 resources destroyed."
