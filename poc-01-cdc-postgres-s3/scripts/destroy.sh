#!/bin/bash
# POC 01 - Destruir stack CloudFormation
# Uso: ./scripts/destroy.sh
#
# ADVERTENCIA: Elimina todos los recursos AWS del POC 01.
# El bucket S3 debe estar vacio para poder eliminarlo.
set -e

STACK_NAME="poc01-cdc-postgres-s3"
REGION="us-east-1"
BUCKET="aws-data-eng-cdc-676206927469"

echo ">>> Deteniendo tarea DMS si esta corriendo..."
TASK_ARN=$(aws dms describe-replication-tasks \
  --region "$REGION" \
  --query "ReplicationTasks[?ReplicationTaskIdentifier=='poc01-cdc-task'].ReplicationTaskArn" \
  --output text 2>/dev/null || true)

if [ -n "$TASK_ARN" ] && [ "$TASK_ARN" != "None" ]; then
  TASK_STATUS=$(aws dms describe-replication-tasks \
    --region "$REGION" \
    --query "ReplicationTasks[?ReplicationTaskIdentifier=='poc01-cdc-task'].Status" \
    --output text)

  if [ "$TASK_STATUS" == "running" ]; then
    echo "    Deteniendo tarea (status: $TASK_STATUS)..."
    aws dms stop-replication-task --replication-task-arn "$TASK_ARN" --region "$REGION" > /dev/null
    echo "    Esperando que se detenga..."
    aws dms wait replication-task-stopped --filters Name=replication-task-arn,Values="$TASK_ARN" --region "$REGION" 2>/dev/null || sleep 20
  else
    echo "    Tarea en status '$TASK_STATUS', no es necesario detenerla."
  fi
else
  echo "    No se encontro tarea DMS activa."
fi

echo ">>> Vaciando bucket S3: $BUCKET (incluyendo versiones)..."
# 1. Suspender versionado para evitar nuevos markers
aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Suspended --region "$REGION" 2>/dev/null || true

# 2. Eliminar objetos actuales
aws s3 rm "s3://$BUCKET" --recursive --region "$REGION" 2>/dev/null || true

# 3. Eliminar versiones y delete markers anteriores
aws s3api list-object-versions --bucket "$BUCKET" --region "$REGION" \
  --query "[Versions,DeleteMarkers][][].{Key:Key,VID:VersionId}" \
  --output text 2>/dev/null | \
while IFS=$'\t' read -r KEY VID; do
  [ -z "$KEY" ] || [ -z "$VID" ] && continue
  aws s3api delete-object --bucket "$BUCKET" --key "$KEY" \
    --version-id "$VID" --region "$REGION" > /dev/null 2>&1 && \
    echo "    deleted: $KEY"
done || echo "    Bucket ya vacio o no existe."

echo ">>> Eliminando stack: $STACK_NAME..."
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"

echo "    Esperando confirmacion de eliminacion (puede tardar ~3 min)..."
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"

echo ""
echo ">>> Stack eliminado. Recursos AWS del POC 01 destruidos."
echo "    Para recrear: bash scripts/deploy.sh"
