"""Order worker for CMPE 273 Lab 5.

Consumes order messages from the "orders" queue on RabbitMQ, processes
each order, and sends an ACK only after processing finishes. Run two
copies of this program against the same queue and RabbitMQ delivers each
message to exactly one of them, so the workers act as competing consumers.

Duplicate handling: every event_id that is processed is appended to
processed_events.txt in the working directory. Before processing a
message, the worker re-reads that file. If the event_id is already there,
the business action is skipped, a duplicate is logged, and the message
is still ACKed so it does not loop forever.

Failure simulation flags (used for the Lab 5 demos, not for normal runs):
    --crash-before-ack:    receive the first message, then exit at once
                           without recording it and without ACKing it.
                           RabbitMQ redelivers it to another worker.
    --crash-after-process: process the first message and record its
                           event_id, then exit without ACKing it. The
                           redelivered copy is then detected as a
                           duplicate by the next worker.

Usage:
    python worker.py --name worker-1
    python worker.py --name worker-2
    python worker.py --name worker-1 --crash-before-ack
    python worker.py --name worker-1 --crash-after-process
"""

import argparse
import json
import os
import sys
import time

import pika

QUEUE_NAME = "orders"
HOST = "localhost"
PROCESSED_FILE = "processed_events.txt"


def already_processed(event_id):
    """Return True if this event_id is recorded in processed_events.txt."""
    if not os.path.exists(PROCESSED_FILE):
        return False
    with open(PROCESSED_FILE, "r") as handle:
        return event_id in {line.strip() for line in handle if line.strip()}


def record_processed(event_id):
    """Append an event_id to processed_events.txt."""
    with open(PROCESSED_FILE, "a") as handle:
        handle.write(event_id + "\n")


def main():
    parser = argparse.ArgumentParser(description="Consume and process order messages.")
    parser.add_argument("--name", default="worker-1", help="Name shown in log lines.")
    parser.add_argument(
        "--crash-before-ack",
        action="store_true",
        help="Exit without ACKing the first message received.",
    )
    parser.add_argument(
        "--crash-after-process",
        action="store_true",
        help="Process the first message, then exit without ACKing it.",
    )
    args = parser.parse_args()
    name = args.name
    crashed = False

    def log(message):
        print(f"[{name}] {message}", flush=True)

    def on_message(channel, method, properties, body):
        nonlocal crashed
        order = json.loads(body)
        event_id = order["event_id"]
        order_id = order["order_id"]
        log(
            f"Received event {event_id} "
            f"(order {order_id}, {order['item_id']} x{order['quantity']})"
        )

        if already_processed(event_id):
            log(f"Duplicate detected for event {event_id}. Business action skipped.")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            log(f"ACK sent for event {event_id} (duplicate, nothing reprocessed)")
            return

        if args.crash_before_ack and not crashed:
            crashed = True
            log(
                f"Simulating a crash: stopping before ACK for event {event_id}. "
                "The message stays unacknowledged."
            )
            sys.stdout.flush()
            os._exit(1)  # process dies; RabbitMQ requeues the message

        log(f"Processing order {order_id}...")
        time.sleep(1)  # stand-in for real order processing work
        record_processed(event_id)
        log(f"Order {order_id} processed.")

        if args.crash_after_process and not crashed:
            crashed = True
            log(
                f"Simulating a crash: stopping after processing but before ACK "
                f"for event {event_id}."
            )
            sys.stdout.flush()
            os._exit(1)  # redelivery will be caught as a duplicate

        channel.basic_ack(delivery_tag=method.delivery_tag)
        log(f"ACK sent for event {event_id}")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    # Give each worker only one unacknowledged message at a time, so work
    # spreads across workers instead of piling up on the fastest one.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)

    log("Waiting for orders. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log("Shutting down.")
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()
