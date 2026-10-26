"""Order producer for CMPE 273 Lab 5.

Publishes order messages to the "orders" queue on RabbitMQ. Each message
is a JSON object with four fields:

    event_id: a unique id for this event, used for duplicate detection
    order_id: the id of the order
    item_id:  the item being ordered
    quantity: how many units were ordered

Usage:
    python producer.py                 # publish the four sample orders
    python producer.py --count 2       # publish only the first two orders
    python producer.py --event EVT-2   # publish just one order, by event_id.
                                       # If that event_id was already
                                       # processed, the worker logs it as
                                       # a duplicate instead of reprocessing it.

The queue is declared durable and every message is marked persistent, so
messages survive a RabbitMQ restart instead of living only in memory.
"""

import argparse
import json
import sys

import pika

QUEUE_NAME = "orders"
HOST = "localhost"

ORDERS = [
    {"event_id": "EVT-1", "order_id": "ORD-1", "item_id": "laptop", "quantity": 2},
    {"event_id": "EVT-2", "order_id": "ORD-2", "item_id": "phone", "quantity": 1},
    {"event_id": "EVT-3", "order_id": "ORD-3", "item_id": "keyboard", "quantity": 3},
    {"event_id": "EVT-4", "order_id": "ORD-4", "item_id": "headphones", "quantity": 1},
]


def main():
    parser = argparse.ArgumentParser(description="Publish order messages to RabbitMQ.")
    parser.add_argument(
        "--count",
        type=int,
        default=len(ORDERS),
        help="How many sample orders to publish (default: all of them).",
    )
    parser.add_argument(
        "--event",
        metavar="EVENT_ID",
        help="Publish one order, selected by its event_id.",
    )
    args = parser.parse_args()

    if args.event:
        messages = [order for order in ORDERS if order["event_id"] == args.event]
        if not messages:
            print(f"[producer] Unknown event_id: {args.event}", file=sys.stderr)
            print(f"[producer] Known event_ids: {', '.join(o['event_id'] for o in ORDERS)}",
                  file=sys.stderr)
            sys.exit(1)
    else:
        if args.count < 1 or args.count > len(ORDERS):
            print(f"[producer] --count must be between 1 and {len(ORDERS)}.", file=sys.stderr)
            sys.exit(1)
        messages = ORDERS[: args.count]

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for order in messages:
        body = json.dumps(order)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        print(
            f"[producer] Sent event {order['event_id']}: "
            f"order {order['order_id']}, {order['item_id']} x{order['quantity']}"
        )

    connection.close()
    print(f"[producer] Done. Published {len(messages)} message(s) to queue '{QUEUE_NAME}'.")


if __name__ == "__main__":
    main()
