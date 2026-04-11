#!/bin/bash
# POC 02 - Deploy CloudFormation
# Uso: ./scripts/deploy.sh
set -e

STACK_NAME="poc02-kinesis-dynamo"
TEMPLATE="cloudformation/poc02-kinesis-dynamo.yml"
REGION="us-east-1"

echo ">>> Desplegando stack: $STACK_NAME"

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$TEMPLATE" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM

echo ">>> Stack desplegado exitosamente"
echo ""
echo ">>> Outputs:"
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" \
  --output table

echo ""
echo ">>> Para enviar eventos:"
echo "    python scripts/producer.py --orders 3"
echo "    python scripts/producer.py --continuous"
