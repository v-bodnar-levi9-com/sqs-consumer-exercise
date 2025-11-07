#!/usr/bin/env python

import collections
import json
import localstack_client.session as boto3

client = boto3.client('sqs')

queue = client.list_queues()["QueueUrls"][0]


event_counts = collections.defaultdict(float)
event_sums = collections.defaultdict(float)

for i in range(100):
    response = client.receive_message(QueueUrl=queue, MaxNumberOfMessages=10)
    if 'Messages' not in response:
        break
    for message in response['Messages']:
        body = message['Body']
        body = json.loads(body)
        if 'type' not in body or 'value' not in body:
            client.delete_message(QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'])
            continue
        message_type = body['type']
        try:
            message_value = float(body['value'])
        except:
            client.delete_message(QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'])
            continue
        event_counts[message_type] += 1
        event_sums[message_type] += message_value
        client.delete_message(QueueUrl=queue, ReceiptHandle=message['ReceiptHandle'])

for key in event_counts:
    print('Event type:', key)
    print('Count:', event_counts[key])
    print('Sum:', event_sums[key])
