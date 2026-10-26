"""Shared connection settings for the producer and workers."""
import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = os.getenv("QUEUE_NAME", "orders")


def get_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    # Durable queue so messages survive a broker restart.
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    return connection, channel
