"""
Lambda — Kinesis Event Processor
Consume eventos de order-events-stream y persiste en DynamoDB
usando single-table design con event sourcing pattern.

Este archivo es la version legible del codigo inline en CloudFormation.
Util para desarrollo local y testing.
"""
import json
import base64
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.resource('dynamodb')
table = dynamo.Table(os.environ['TABLE_NAME'])


def handler(event, context):
    ok, fail = 0, 0

    for rec in event['Records']:
        try:
            # Kinesis entrega los datos en base64
            raw = base64.b64decode(rec['kinesis']['data'])
            evt = json.loads(raw)

            # Single-table design:
            # PK = ORDER#<order_id>  -> agrupa todos los eventos de una orden
            # SK = EVENT#<ts>#<type> -> ordena eventos cronologicamente
            table.put_item(Item={
                'PK':          f"ORDER#{evt['order_id']}",
                'SK':          f"EVENT#{evt['timestamp']}#{evt['event_type']}",
                # GSI para consultar ordenes de un cliente
                'GSI1PK':      f"CUSTOMER#{evt['customer_id']}",
                'GSI1SK':      evt['timestamp'],
                # Atributos del evento
                'event_id':    evt['event_id'],
                'order_id':    evt['order_id'],
                'customer_id': str(evt['customer_id']),
                'event_type':  evt['event_type'],
                'product':     evt.get('product', ''),
                'amount':      str(evt.get('amount', 0)),
                'status':      evt['status'],
                'timestamp':   evt['timestamp'],
            })
            ok += 1
            logger.info(f"OK  [{evt['event_type']}] order={evt['order_id']}")

        except Exception as e:
            logger.error(f"ERR processing record: {e}")
            fail += 1

    logger.info(f"Batch complete: processed={ok} failed={fail}")
    return {'processed': ok, 'failed': fail}
