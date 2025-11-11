#!/usr/bin/env python
import collections
import json
import os
import localstack_client.session as boto3

client = boto3.client('sqs')

QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "hands-on-interview")

def get_queue_url(queue_name):
    try:
        res = client.get_queue_url(QueueName=queue_name)
    except Exception:  # broad except kept; TODO narrow exception types
        res = client.create_queue(QueueName=queue_name)
    return res["QueueUrl"]

def process_messages(queue_url, event_counts, event_sums):
    """Receive messages from SQS queue and update counters."""
    response = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20  # SQS Long Polling
    )
    if 'Messages' not in response:
        return
    for message in response['Messages']:
        body = message['Body']
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            continue
        if 'type' not in body or 'value' not in body:
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            continue
        message_type = body['type']
        try:
            message_value = float(body['value'])
        except (TypeError, ValueError):
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            continue
        event_counts[message_type] += 1
        event_sums[message_type] += message_value
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])

def print_stats(event_counts, event_sums):
    for key in event_counts:
        print('Event type:', key)
        print('Count:', event_counts[key])
        print('Sum:', event_sums[key])

def main():
    queue_url = get_queue_url(QUEUE_NAME)
    event_counts = collections.defaultdict(float)
    event_sums = collections.defaultdict(float)
    while True:  # TODO: Replace with proper loop and exit conditions
        print("Fetching messages from queue_url...")  # TODO use logging
        process_messages(queue_url, event_counts, event_sums)
        print_stats(event_counts, event_sums)

if __name__ == "__main__":
    main()
