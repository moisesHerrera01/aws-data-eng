#!/bin/bash
# POC 02 - Destruir stack CloudFormation
# Uso: ./scripts/destroy.sh
set -e

STACK_NAME="poc02-kinesis-dynamo"
REGION="us-east-1"
TABLE="order-events"

echo ">>> Vaciando tabla DynamoDB: $TABLE..."
# DynamoDB no requiere vaciarse antes de eliminar el stack
# pero lo hacemos para evitar costos de almacenamiento residual
ITEMS=$(aws dynamodb scan --table-name "$TABLE" --region "$REGION" \
  --query "count(Items)" --output text 2>/dev/null || echo "0")
echo "    Items en tabla: $ITEMS (se eliminaran con el stack)"

echo ">>> Eliminando stack: $STACK_NAME..."
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"

echo "    Esperando confirmacion (puede tardar ~2 min)..."
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"

echo ""
echo ">>> Stack eliminado. Recursos AWS del POC 02 destruidos."
echo "    Para recrear: bash scripts/deploy.sh"
