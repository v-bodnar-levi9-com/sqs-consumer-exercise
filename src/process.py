import collections
import json
import logging
import os
import localstack_client.session as boto3
from pydantic import ValidationError

from .schemas import SQSMessageBody

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)


logger = logging.getLogger(__name__)
client = boto3.client('sqs')


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
        receipt_handle = message['ReceiptHandle']
        
        try:
            body_dict = json.loads(body)
        except json.JSONDecodeError as e:
            logger.warning(f"Received message with invalid JSON: {e}, deleting message")
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue
        
        try:
            message_data = SQSMessageBody(**body_dict)
            logger.debug(f"Successfully validated message: type={message_data.type}, value={message_data.value}")
        except ValidationError as e:
            logger.warning(f"Received message with invalid schema: {e}, deleting message")
            client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            continue
        
        message_type = message_data.type
        message_value = float(message_data.value)
        
        event_counts[message_type] += 1
        event_sums[message_type] += message_value
        logger.debug(f"Processed message: type={message_type}, value={message_value}")
        client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
