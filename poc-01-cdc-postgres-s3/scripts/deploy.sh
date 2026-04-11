#!/bin/bash
# POC 01 - Deploy CloudFormation
# Uso: ./scripts/deploy.sh
#
# Prerequisito — crear el secreto en SSM (solo la primera vez):
#   aws ssm put-parameter --name "poc01-postgres-password" --value "TU_PASSWORD" --type SecureString
#
# Para cambiar el puerto ngrok, edita NGROK_PORT abajo.
set -e

STACK_NAME="poc01-cdc-postgres-s3"
TEMPLATE="cloudformation/poc01-cdc.yml"
REGION="us-east-1"

# --- Parametros no sensibles (edita aqui cuando cambie el tunel ngrok) ---
NGROK_HOST="6.tcp.ngrok.io"
NGROK_PORT="18009"
PG_USER="pguser"
PG_DATABASE="salesdb"
# -------------------------------------------------------------------------

echo ">>> Obteniendo credenciales desde SSM..."
PG_PASSWORD=$(aws ssm get-parameter \
  --name "poc01-postgres-password" \
  --with-decryption \
  --region "$REGION" \
  --query "Parameter.Value" \
  --output text)

echo ">>> Desplegando stack: $STACK_NAME"

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$TEMPLATE" \
  --region "$REGION" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    NgrokHost="$NGROK_HOST" \
    NgrokPort="$NGROK_PORT" \
    PostgresUser="$PG_USER" \
    PostgresDatabase="$PG_DATABASE" \
    PostgresPassword="$PG_PASSWORD"

echo ">>> Stack desplegado exitosamente"
echo ""
echo ">>> Outputs:"
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" \
  --output table
