#!/bin/bash
# POC 03 - Deploy CloudFormation
set -e

STACK_NAME="poc03-emr-spark"
TEMPLATE="cloudformation/poc03-emr-spark.yml"
REGION="us-east-1"

echo ">>> Deploying stack: $STACK_NAME"
aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$TEMPLATE" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM

echo ">>> Stack deployed"
echo ""
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" --region "$REGION" \
  --query "Stacks[0].Outputs" --output table
