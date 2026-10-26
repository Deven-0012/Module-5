"""Order Worker: consumes order messages, processes them, then ACKs.

Usage:
    python worker.py --name worker-1
    python worker.py --name worker-2 --crash-after 2   # die before ACKing its 2nd message
    python worker.py --name worker-2 --crash-after 1 --crash-after-processing
                                                       # do the work, then die before ACK

Processed event_ids are stored in a shared file (processed_events.txt) so that
duplicate detection works across worker processes and restarts.
"""
import argparse
import fcntl
import json
import os
import time
from datetime import datetime, timezone

from common import QUEUE_NAME, get_channel

PROCESSED_FILE = os.getenv("PROCESSED_FILE", "processed_events.txt")


def log(name, msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] [{name}] {msg}", flush=True)


def claim_event(event_id):
    """Atomically record event_id. Returns False if it was already processed."""
    with open(PROCESSED_FILE, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)  # serialize access between worker processes
        f.seek(0)
        seen = {line.strip() for line in f}
        if event_id in seen:
            return False
        f.write(event_id + "\n")
        f.flush()
        return True


def main():
    parser = argparse.ArgumentParser(description="Order worker")
    parser.add_argument("--name", default=f"worker-{os.getpid()}")
    parser.add_argument("--crash-after", type=int, metavar="N",
                        help="simulate a crash (exit without ACK) on the N-th message received")
    parser.add_argument("--crash-after-processing", action="store_true",
                        help="with --crash-after: crash AFTER the business action but before the ACK")
    parser.add_argument("--work-seconds", type=float, default=1.0, help="simulated processing time")
    args = parser.parse_args()

    connection, channel = get_channel()
    # Give each worker one unacked message at a time -> fair round-robin dispatch.
    channel.basic_qos(prefetch_count=1)

    received = 0

    def on_message(ch, method, properties, body):
        nonlocal received
        received += 1
        order = json.loads(body)
        event_id = order["event_id"]
        log(args.name, f"received order_id={order['order_id']} event_id={event_id} "
                       f"redelivered={method.redelivered}")

        crash_now = args.crash_after and received == args.crash_after

        if crash_now and not args.crash_after_processing:
            log(args.name, f"!!! simulated CRASH while processing {order['order_id']} "
                           f"- exiting WITHOUT ack")
            os._exit(1)  # hard exit: no ack, connection drops, broker requeues

        if not claim_event(event_id):
            log(args.name, f"DUPLICATE detected event_id={event_id} - skipping business action")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        log(args.name, f"processing order_id={order['order_id']} item_id={order['item_id']} "
                       f"quantity={order['quantity']}")
        time.sleep(args.work_seconds)  # simulated business work

        if crash_now:
            log(args.name, f"!!! simulated CRASH after processing {order['order_id']} "
                           f"- exiting WITHOUT ack")
            os._exit(1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        log(args.name, f"done order_id={order['order_id']} -> ACK sent")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log(args.name, f"waiting for messages on queue '{QUEUE_NAME}'. CTRL+C to exit")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()
