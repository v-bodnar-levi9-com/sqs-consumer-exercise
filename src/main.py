#!/usr/bin/env python
import collections
import json
import logging
import os
import localstack_client.session as boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sqs_consumer.log')
    ]
)

logger = logging.getLogger(__name__)

client = boto3.client('sqs')

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "hands-on-interview")

def get_queue_url(queue_name):
    try:
        logger.info(f"Getting queue URL for queue: {queue_name}")
        res = client.get_queue_url(QueueName=queue_name)
        logger.info(f"Successfully retrieved queue URL: {res['QueueUrl']}")
    except Exception:  # broad except kept; TODO narrow exception types
        logger.info(f"Queue {queue_name} not found, creating new queue")
        res = client.create_queue(QueueName=queue_name)
        logger.info(f"Successfully created queue with URL: {res['QueueUrl']}")
    return res["QueueUrl"]

def process_messages(queue_url, event_counts, event_sums):
    """Receive messages from SQS queue and update counters."""
    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20  # SQS Long Polling
    )
    if 'Messages' not in response:
        logger.debug("No messages received from queue")
        return
    
    logger.info(f"Received {len(response['Messages'])} messages from queue")
    
    for message in response['Messages']:
        body = message['Body']
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("Received message with invalid JSON, deleting message")
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            continue
        if 'type' not in body or 'value' not in body:
            logger.warning("Received message missing required fields 'type' or 'value', deleting message")
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            continue
        message_type = body['type']
        try:
            message_value = float(body['value'])
        except (TypeError, ValueError):
            logger.warning(f"Received message with invalid value: {body['value']}, deleting message")
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            continue
        event_counts[message_type] += 1
        event_sums[message_type] += message_value
        logger.debug(f"Processed message: type={message_type}, value={message_value}")
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])

def print_stats(event_counts, event_sums):
    for key in event_counts:
        logger.info(f'Event type: {key}')
        logger.info(f'Count: {event_counts[key]}')
        logger.info(f'Sum: {event_sums[key]}')

def main():
    logger.info("Starting SQS consumer application")
    queue_url = get_queue_url(QUEUE_NAME)
    event_counts = collections.defaultdict(float)
    event_sums = collections.defaultdict(float)
    logger.info("Starting message processing loop")
    while True:  # TODO: Replace with proper loop and exit conditions
        logger.info("Fetching messages from queue_url...")
        process_messages(queue_url, event_counts, event_sums)
        print_stats(event_counts, event_sums)

if __name__ == "__main__":
    main()
