#!/usr/bin/env python

import json
import os
import random
import string
import logging
from datetime import datetime, timedelta
import time

import localstack_client.session as boto3

# Configure logging based on LOG_LEVEL environment variable
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "hands-on-interview")
FORMAT = "%Y-%m-%d %H:%M:%S"
DIFFERENT_FORMAT = "%m/%d/%Y %H:%M"

PAGEVIEW = ("pageview", lambda: 1)
ADDED_TO_BASKET = ("added-to-basket", lambda: random.randint(1, 5))
PURCHASE = ("purchase", lambda: round(100 * random.random(), 2))
EVENTS = (PAGEVIEW, PAGEVIEW, PAGEVIEW, ADDED_TO_BASKET, ADDED_TO_BASKET, PURCHASE)

client = boto3.client("sqs")


def get_queue_url(queue_name):
    try:
        res = client.get_queue_url(QueueName=queue_name)
        logger.info("Retrieved queue URL")
    except:
        res = client.create_queue(QueueName=queue_name)
        logger.info("Created queue")
    return res["QueueUrl"]


def get_time(seconds_ago=0, date_format=FORMAT):
    delta = datetime.now() - timedelta(seconds=seconds_ago)
    return delta.strftime(date_format)


def get_perfect_message(delta=0):
    event, value_function = random.choice(EVENTS)
    return {"type": event, "value": value_function(), "occurred_at": get_time(delta)}


def get_missing_field():
    message = get_perfect_message()
    key_to_delete = random.choice(list(message.keys()))
    del message[key_to_delete]
    return message


def get_wrong_type():
    message = get_perfect_message()
    key = random.choice(["value", "occurred_at"])
    message[key] = "".join(random.choices(string.ascii_letters, k=5))
    return message


def send_message(msg, queue_url):
    res = client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))
    return res["ResponseMetadata"]["HTTPStatusCode"] == 200


def get_num_messages(queue_url):
    res = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )
    return int(res["Attributes"]["ApproximateNumberOfMessages"])


def draw_scenario():
    return [
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(),
        get_perfect_message(delta=3600),
        get_perfect_message(delta=3600),
        get_perfect_message(delta=3600),
        get_perfect_message(delta=3600),
        get_missing_field(),
        get_missing_field(),
        get_missing_field(),
        get_missing_field(),
        get_wrong_type(),
        get_wrong_type(),
        get_wrong_type(),
    ]


if __name__ == "__main__":
    logger.info("Starting event server (producer)...")
    queue_url = get_queue_url(QUEUE_NAME)
    amount_of_scenarios = int(os.getenv("ITERATIONS", 10))
    while True:

        messages_on_queue = get_num_messages(queue_url)
        logger.debug(f"{messages_on_queue=}")
        if messages_on_queue == 0:
            for i in range(amount_of_scenarios):
                messages = draw_scenario()
                logger.debug(f"Preparing to send a scenario with {len(messages)}")
                sent = sum([1 for msg in messages if send_message(msg, queue_url)])
                logger.debug(f"Sent {sent} out of {len(messages)}")
                logger.debug("-" * 35)

        time.sleep(5)
