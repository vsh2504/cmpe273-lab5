# CMPE 273 Lab 5: Asynchronous Messaging with RabbitMQ

This lab builds a small asynchronous order system in Python. It has two
components that run as independent processes: an order producer that
publishes order messages, and order workers that consume and process
them. The producer and the workers never call each other directly. They
only talk through a RabbitMQ queue, so either side can run, stop, or
scale without the other side changing.

## Architecture

```mermaid
flowchart LR
    P[Order Producer<br/>producer.py] --> Q[(RabbitMQ queue<br/>"orders")]
    Q --> W1[Order Worker 1<br/>worker.py --name worker-1]
    Q --> W2[Order Worker 2<br/>worker.py --name worker-2]
    W1 --> F[(processed_events.txt<br/>processed event_ids)]
    W2 --> F
```

How it works:

1. The producer publishes order messages to the queue named `orders`.
2. Two worker processes consume from the same queue. RabbitMQ delivers
   each message to only one worker, so the workers are competing
   consumers and the work spreads across them.
3. A worker sends an ACK only after it finishes processing a message.
   Until the ACK arrives, RabbitMQ keeps the message and redelivers it
   if the worker dies.
4. Before processing, a worker checks `processed_events.txt`, a file
   shared by both workers that lists every event_id already processed.
   If the event_id is already there, the worker logs a duplicate, skips
   the business action, and still ACKs the message.

The queue is declared durable and every message is published as
persistent, so messages survive a RabbitMQ restart. Each worker sets
`prefetch_count=1`, which means RabbitMQ gives a worker only one
unacknowledged message at a time. Without this, one fast worker could
grab all the messages before the other worker gets any.

## Message Format

Each message is a JSON object with four fields:

```json
{"event_id": "EVT-1", "order_id": "ORD-1", "item_id": "laptop", "quantity": 2}
```

| Field | Meaning |
| --- | --- |
| `event_id` | Unique id for this event. Used to detect duplicates. |
| `order_id` | Id of the order being placed. |
| `item_id` | The item being ordered. |
| `quantity` | How many units were ordered. |

The producer ships four sample orders: EVT-1 (ORD-1, laptop x2),
EVT-2 (ORD-2, phone x1), EVT-3 (ORD-3, keyboard x3), and EVT-4
(ORD-4, headphones x1).

## Setup

### 1. Run RabbitMQ

Pick one option. The code connects to RabbitMQ on `localhost:5672`
with the default guest account, so no extra configuration is needed.

macOS (Homebrew):

```bash
brew install rabbitmq
brew services start rabbitmq-server
```

Ubuntu / Debian:

```bash
sudo apt install rabbitmq-server
sudo service rabbitmq-server start
```

Docker:

```bash
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management
```

Check that the broker is up:

```bash
rabbitmqctl status
```

### 2. Install the Python dependency

The only dependency is `pika`, the Python client for RabbitMQ.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running the System

Run each command from the repository root, in its own terminal. The
workers must run from the repository root because they share
`processed_events.txt` from the working directory.

Terminal 1, worker 1:

```bash
python worker.py --name worker-1
```

Terminal 2, worker 2:

```bash
python worker.py --name worker-2
```

Terminal 3, producer:

```bash
python producer.py
```

Useful producer options:

```bash
python producer.py --count 2     # publish only the first two orders
python producer.py --event EVT-2 # publish just one order, by event_id
```

To start a demo from a clean state, stop the workers, delete the
dedupe file, and empty the queue:

```bash
rm -f processed_events.txt
rabbitmqctl purge_queue orders
```

## Results

### 1. Successful Message Processing

Publishing the four sample orders:

```text
$ python producer.py
[producer] Sent event EVT-1: order ORD-1, laptop x2
[producer] Sent event EVT-2: order ORD-2, phone x1
[producer] Sent event EVT-3: order ORD-3, keyboard x3
[producer] Sent event EVT-4: order ORD-4, headphones x1
[producer] Done. Published 4 message(s) to queue 'orders'.
```

Each worker logs the order it receives, processes it, and ACKs it
only after processing finishes. This is the worker 1 log from the run
above:

```text
[worker-1] Waiting for orders. To exit press CTRL+C
[worker-1] Received event EVT-1 (order ORD-1, laptop x2)
[worker-1] Processing order ORD-1...
[worker-1] Order ORD-1 processed.
[worker-1] ACK sent for event EVT-1
[worker-1] Received event EVT-3 (order ORD-3, keyboard x3)
[worker-1] Processing order ORD-3...
[worker-1] Order ORD-3 processed.
[worker-1] ACK sent for event EVT-3
```

### 2. Two Competing Consumers

Both workers were connected to the same queue when the four orders
were published. The messages split across the workers, and no message
was handled by both workers:

| Worker | Events handled |
| --- | --- |
| worker-1 | EVT-1, EVT-3 |
| worker-2 | EVT-2, EVT-4 |

Worker 2 log from the same run:

```text
[worker-2] Waiting for orders. To exit press CTRL+C
[worker-2] Received event EVT-2 (order ORD-2, phone x1)
[worker-2] Processing order ORD-2...
[worker-2] Order ORD-2 processed.
[worker-2] ACK sent for event EVT-2
[worker-2] Received event EVT-4 (order ORD-4, headphones x1)
[worker-2] Processing order ORD-4...
[worker-2] Order ORD-4 processed.
[worker-2] ACK sent for event EVT-4
```

After the run, the queue was empty, which confirms every message was
consumed and acknowledged exactly once by one worker:

```text
$ rabbitmqctl list_queues name messages_ready messages_unacknowledged
name    messages_ready  messages_unacknowledged
orders  0               0
```

### 3. Failure and Redelivery

This demo uses the `--crash-before-ack` flag, which makes a worker
exit right after receiving a message, before sending the ACK. Only
worker-1 was running when EVT-1 was published:

```text
$ python worker.py --name worker-1 --crash-before-ack
[worker-1] Waiting for orders. To exit press CTRL+C
[worker-1] Received event EVT-1 (order ORD-1, laptop x2)
[worker-1] Simulating a crash: stopping before ACK for event EVT-1. The message stays unacknowledged.
```

Worker-1 is gone, and the message is back in the queue as ready
instead of lost, because it was never acknowledged:

```text
$ rabbitmqctl list_queues name messages_ready messages_unacknowledged
name    messages_ready  messages_unacknowledged
orders  1               0
```

Starting worker-2 redelivers the same message to it, and worker-2
processes and ACKs it:

```text
$ python worker.py --name worker-2
[worker-2] Waiting for orders. To exit press CTRL+C
[worker-2] Received event EVT-1 (order ORD-1, laptop x2)
[worker-2] Processing order ORD-1...
[worker-2] Order ORD-1 processed.
[worker-2] ACK sent for event EVT-1
```

The system stayed running after the crash. Publishing a second order
was handled normally by the surviving worker:

```text
$ python producer.py --event EVT-2
[producer] Sent event EVT-2: order ORD-2, phone x1
[producer] Done. Published 1 message(s) to queue 'orders'.

[worker-2] Received event EVT-2 (order ORD-2, phone x1)
[worker-2] Processing order ORD-2...
[worker-2] Order ORD-2 processed.
[worker-2] ACK sent for event EVT-2
```

### 4. Duplicate Handling

This demo uses the `--crash-after-process` flag, which makes a worker
process a message and record its event_id, then exit before sending
the ACK. That is the realistic failure: the work is done, but
RabbitMQ does not know it, so the message is delivered again.

Worker-1 processed EVT-1, recorded it, and then crashed before the ACK:

```text
$ python worker.py --name worker-1 --crash-after-process
[worker-1] Waiting for orders. To exit press CTRL+C
[worker-1] Received event EVT-1 (order ORD-1, laptop x2)
[worker-1] Processing order ORD-1...
[worker-1] Order ORD-1 processed.
[worker-1] Simulating a crash: stopping after processing but before ACK for event EVT-1.
```

When worker-2 started, it received the redelivered copy, saw EVT-1
in `processed_events.txt`, and skipped the business action:

```text
$ python worker.py --name worker-2
[worker-2] Waiting for orders. To exit press CTRL+C
[worker-2] Received event EVT-1 (order ORD-1, laptop x2)
[worker-2] Duplicate detected for event EVT-1. Business action skipped.
[worker-2] ACK sent for event EVT-1 (duplicate, nothing reprocessed)
```

Publishing EVT-1 a second time on purpose produced the same result,
so the protection works for producer replays too, not just for
redeliveries:

```text
$ python producer.py --event EVT-1
[producer] Sent event EVT-1: order ORD-1, laptop x2
[producer] Done. Published 1 message(s) to queue 'orders'.

[worker-2] Received event EVT-1 (order ORD-1, laptop x2)
[worker-2] Duplicate detected for event EVT-1. Business action skipped.
[worker-2] ACK sent for event EVT-1 (duplicate, nothing reprocessed)
```

The order was processed once even though it was delivered three
times. `processed_events.txt` contains EVT-1 exactly once:

```text
$ cat processed_events.txt
EVT-1
```

Note that the duplicate is still ACKed. If it were not, RabbitMQ
would keep redelivering it forever. Acknowledging a duplicate removes
it from the queue without reprocessing it.

## Reflection

A message can be delivered more than once because RabbitMQ only
removes a message from the queue after it receives an ACK. If a worker
crashes after doing the work but before sending the ACK, RabbitMQ
cannot know the work already happened, so it redelivers the message
to another worker. The same thing happens if a producer publishes the
same event twice. Idempotent handling fixes this by checking a stored
event_id before doing the business action, so a repeated delivery is
recognized and skipped instead of being processed again. The result is
that the system can safely redeliver messages without ever charging,
shipping, or counting the same order twice.

## Files

| File | Purpose |
| --- | --- |
| `producer.py` | Publishes order messages to the `orders` queue. |
| `worker.py` | Consumes messages, processes orders, ACKs after processing, and detects duplicates. |
| `requirements.txt` | Python dependency (`pika`). |
| `processed_events.txt` | Created at runtime, not committed. Lists processed event_ids. |
