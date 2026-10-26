"""Order Producer: publishes order messages to the RabbitMQ 'orders' queue.

Usage:
    python producer.py --count 10                 # 10 new orders (ORD-1001..ORD-1010)
    python producer.py --duplicate-of <event_id>  # re-publish an existing event_id
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timezone

import pika

from common import QUEUE_NAME, get_channel


def build_order(event_id=None, order_id=None):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "order_id": order_id or f"ORD-{random.randint(1000, 9999)}",
        "item_id": f"ITEM-{random.randint(1, 50):03d}",
        "quantity": random.randint(1, 5),
    }


def publish(channel, order):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(order),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] [producer] published {json.dumps(order)}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=10, help="number of new orders to publish")
    parser.add_argument("--start", type=int, default=1001, help="first order number")
    parser.add_argument("--duplicate-of", metavar="EVENT_ID",
                        help="publish one message re-using this event_id (simulates a duplicate)")
    args = parser.parse_args()

    connection, channel = get_channel()
    if args.duplicate_of:
        publish(channel, build_order(event_id=args.duplicate_of, order_id="ORD-DUPLICATE"))
    else:
        for n in range(args.start, args.start + args.count):
            publish(channel, build_order(order_id=f"ORD-{n}"))
    connection.close()


if __name__ == "__main__":
    main()
