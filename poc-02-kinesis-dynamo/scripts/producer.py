"""
Producer — Simulador de eventos de ordenes
Envia eventos al Kinesis Data Stream simulando el ciclo de vida de ordenes.

Uso:
  python producer.py                  # envia 1 orden completa
  python producer.py --orders 5       # envia 5 ordenes completas
  python producer.py --continuous     # envia eventos indefinidamente
"""
import boto3
import json
import uuid
import time
import argparse
import random
from datetime import datetime, timezone

STREAM_NAME = "order-events-stream"
REGION = "us-east-1"

PRODUCTS = [
    {"name": "Laptop",    "amount": 1200.00},
    {"name": "Monitor",   "amount": 350.00},
    {"name": "Keyboard",  "amount": 89.99},
    {"name": "Headset",   "amount": 149.99},
    {"name": "Webcam",    "amount": 79.99},
    {"name": "Mouse",     "amount": 45.00},
    {"name": "Tablet",    "amount": 499.00},
    {"name": "Docking Station", "amount": 199.99},
]

# Ciclo de vida de una orden (en orden)
ORDER_LIFECYCLE = [
    "order_placed",
    "order_confirmed",
    "order_shipped",
    "order_delivered",
]

# Mapa de estado por evento
STATUS_MAP = {
    "order_placed":    "pending",
    "order_confirmed": "confirmed",
    "order_shipped":   "shipped",
    "order_delivered": "delivered",
    "order_cancelled": "cancelled",
}

kinesis = boto3.client('kinesis', region_name=REGION)


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def send_event(order_id, customer_id, event_type, product, amount):
    event = {
        "event_id":    str(uuid.uuid4()),
        "order_id":    order_id,
        "customer_id": customer_id,
        "event_type":  event_type,
        "product":     product,
        "amount":      amount,
        "status":      STATUS_MAP[event_type],
        "timestamp":   now_iso(),
    }

    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event).encode('utf-8'),
        PartitionKey=order_id,  # misma particion para eventos de la misma orden
    )

    shard = response['ShardId']
    seq   = response['SequenceNumber'][:12] + "..."
    print(f"  [{event_type:20s}] order={order_id[:8]}... shard={shard} seq={seq}")
    return event


def simulate_order(customer_id=None, cancel=False):
    order_id    = str(uuid.uuid4())
    customer_id = customer_id or random.randint(1, 5)
    product     = random.choice(PRODUCTS)

    print(f"\n>>> Nueva orden: {order_id[:8]}... | customer={customer_id} | product={product['name']}")

    lifecycle = ORDER_LIFECYCLE.copy()
    if cancel:
        # Cancela despues de confirmar
        lifecycle = ["order_placed", "order_confirmed", "order_cancelled"]

    for event_type in lifecycle:
        send_event(order_id, customer_id, event_type, product['name'], product['amount'])
        time.sleep(0.5)  # simula tiempo entre eventos

    return order_id


def main():
    parser = argparse.ArgumentParser(description="Kinesis Order Event Producer")
    parser.add_argument('--orders',     type=int,  default=1,     help='Numero de ordenes a simular')
    parser.add_argument('--continuous', action='store_true',       help='Enviar eventos continuamente')
    parser.add_argument('--cancel-rate',type=float, default=0.2,  help='Tasa de cancelacion (0.0-1.0)')
    args = parser.parse_args()

    print(f"Produciendo eventos en stream: {STREAM_NAME} | region: {REGION}")

    if args.continuous:
        print("Modo continuo — Ctrl+C para detener\n")
        count = 0
        try:
            while True:
                cancel = random.random() < args.cancel_rate
                simulate_order(cancel=cancel)
                count += 1
                print(f"  Total ordenes enviadas: {count}")
                time.sleep(2)
        except KeyboardInterrupt:
            print(f"\nDetenido. Total ordenes: {count}")
    else:
        for i in range(args.orders):
            cancel = random.random() < args.cancel_rate
            simulate_order(cancel=cancel)
        print(f"\nListo. {args.orders} orden(es) enviada(s) a Kinesis.")


if __name__ == "__main__":
    main()
