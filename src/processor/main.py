import asyncio
import json
import logging
import signal
import sys
import time

import localstack_client.session as boto3
from pydantic import ValidationError

from ..shared.config import Config
from ..shared.redis_client import redis_client
from ..shared.schemas import SQSMessageBody

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class SQSProcessor:
    """Scalable SQS message processor that writes stats to Redis"""

    def __init__(self):
        self.running = True
        self.sqs_client = boto3.client("sqs")
        self.queue_url = None
        self.dlq_url = None

        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)

    def _shutdown_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    async def _get_queue_url(self):
        """Get or create SQS queue URL with DLQ configuration"""
        if self.queue_url is None:
            try:
                # First, setup the DLQ
                await self._setup_dlq()

                logger.info(f"Getting queue URL for queue: {Config.SQS_QUEUE_NAME}")
                res = self.sqs_client.get_queue_url(QueueName=Config.SQS_QUEUE_NAME)
                self.queue_url = res["QueueUrl"]
                logger.info(f"Successfully retrieved queue URL: {self.queue_url}")

                # Configure the main queue with DLQ settings
                await self._configure_queue_dlq()

            except Exception:
                logger.info(
                    f"Queue {Config.SQS_QUEUE_NAME} not found, creating new queue with DLQ configuration"
                )

                # Setup DLQ first if it doesn't exist
                await self._setup_dlq()

                # Create main queue with redrive policy
                attributes = {
                    "VisibilityTimeout": str(Config.SQS_VISIBILITY_TIMEOUT),
                    "RedrivePolicy": json.dumps(
                        {
                            "deadLetterTargetArn": await self._get_dlq_arn(),
                            "maxReceiveCount": Config.SQS_MAX_RECEIVE_COUNT,
                        }
                    ),
                }

                res = self.sqs_client.create_queue(
                    QueueName=Config.SQS_QUEUE_NAME, Attributes=attributes
                )
                self.queue_url = res["QueueUrl"]
                logger.info(f"Successfully created queue with URL: {self.queue_url}")
        return self.queue_url

    async def _setup_dlq(self):
        """Setup Dead Letter Queue"""
        try:
            res = self.sqs_client.get_queue_url(QueueName=Config.DLQ_QUEUE_NAME)
            self.dlq_url = res["QueueUrl"]
            logger.info(f"DLQ already exists: {self.dlq_url}")
        except Exception:
            logger.info(f"Creating DLQ: {Config.DLQ_QUEUE_NAME}")
            res = self.sqs_client.create_queue(QueueName=Config.DLQ_QUEUE_NAME)
            self.dlq_url = res["QueueUrl"]
            logger.info(f"Successfully created DLQ: {self.dlq_url}")

    async def _get_dlq_arn(self):
        """Get the ARN of the Dead Letter Queue"""
        if not self.dlq_url:
            await self._setup_dlq()

        attributes = self.sqs_client.get_queue_attributes(
            QueueUrl=self.dlq_url, AttributeNames=["QueueArn"]
        )
        return attributes["Attributes"]["QueueArn"]

    async def _configure_queue_dlq(self):
        """Configure the main queue with DLQ settings if not already configured"""
        try:
            # Get current attributes
            current_attrs = self.sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url, AttributeNames=["All"]
            )

            # Check if redrive policy is already configured
            if "RedrivePolicy" not in current_attrs.get("Attributes", {}):
                logger.info("Configuring queue with DLQ settings")
                dlq_arn = await self._get_dlq_arn()

                attributes = {
                    "VisibilityTimeout": str(Config.SQS_VISIBILITY_TIMEOUT),
                    "RedrivePolicy": json.dumps(
                        {
                            "deadLetterTargetArn": dlq_arn,
                            "maxReceiveCount": Config.SQS_MAX_RECEIVE_COUNT,
                        }
                    ),
                }

                self.sqs_client.set_queue_attributes(
                    QueueUrl=self.queue_url, Attributes=attributes
                )
                logger.info("Successfully configured queue with DLQ settings")
            else:
                logger.info("Queue already has DLQ configuration")

        except Exception as e:
            logger.warning(f"Could not configure queue DLQ settings: {e}")

    def _extend_message_visibility(self, receipt_handle, extend_seconds=None):
        """Extend the visibility timeout of a message during processing"""
        if extend_seconds is None:
            extend_seconds = Config.SQS_VISIBILITY_TIMEOUT

        try:
            self.sqs_client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=extend_seconds,
            )
            logger.debug(f"Extended message visibility by {extend_seconds} seconds")
        except Exception as e:
            logger.warning(f"Failed to extend message visibility: {e}")

    async def _wait_for_redis_connection(self, max_retries=30):
        """Wait for Redis connection with exponential backoff"""
        for attempt in range(max_retries):
            try:
                if redis_client.ping():
                    logger.info("Successfully connected to Redis")
                    return True
            except Exception as e:
                logger.warning(
                    f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                # Use exponential backoff instead of fixed sleep
                wait_time = min(2**attempt * 0.1, 5)  # Max 5 seconds
                await asyncio.sleep(wait_time)

        logger.error("Failed to connect to Redis after maximum retries")
        return False

    def process_messages(self):
        """Process messages from SQS queue and update Redis stats"""
        queue_url = self.queue_url

        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=Config.MAX_MESSAGES_PER_BATCH,
                WaitTimeSeconds=Config.SQS_WAIT_TIME_SECONDS,
                AttributeNames=["ApproximateReceiveCount"],  # Track receive count
                VisibilityTimeout=Config.SQS_VISIBILITY_TIMEOUT,
            )

            if "Messages" not in response:
                logger.debug("No messages received from queue")
                return 0

            messages = response["Messages"]
            logger.info(f"Received {len(messages)} messages from queue")

            processed_count = 0

            for message in messages:
                if not self.running:
                    logger.info("Shutdown requested, stopping message processing")
                    break

                body = message["Body"]
                receipt_handle = message["ReceiptHandle"]

                # Get message attributes for monitoring
                attributes = message.get("Attributes", {})
                receive_count = int(attributes.get("ApproximateReceiveCount", "1"))

                logger.debug(f"Processing message (receive count: {receive_count})")

                # For messages that have been received multiple times, extend visibility
                # to give more time for processing
                if receive_count > 1:
                    logger.warning(f"Message has been received {receive_count} times")
                    # Extend visibility timeout for retry attempts
                    self._extend_message_visibility(receipt_handle)

                try:
                    # Parse JSON
                    body_dict = json.loads(body)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Received message with invalid JSON: {e}, deleting message"
                    )
                    self.sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=receipt_handle
                    )
                    continue

                try:
                    # Validate schema
                    message_data = SQSMessageBody(**body_dict)
                    logger.debug(
                        f"Successfully validated message: type={message_data.type}, value={message_data.value}"
                    )
                except ValidationError as e:
                    logger.warning(
                        f"Received message with invalid schema: {e}, deleting message"
                    )
                    self.sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=receipt_handle
                    )
                    continue

                try:
                    # For long-running processing, we might need to extend visibility
                    # In this case, Redis operations are fast, but this is a pattern
                    # to follow for more complex processing

                    # Update Redis stats
                    redis_client.increment_event(
                        message_data.type, float(message_data.value)
                    )

                    # Delete message from SQS after successful processing
                    self.sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=receipt_handle
                    )

                    processed_count += 1
                    logger.debug(
                        f"Processed message: type={message_data.type}, value={message_data.value}"
                    )

                except Exception as e:
                    logger.error(
                        f"Error processing message (receive count: {receive_count}): {e}"
                    )

                    # Don't delete the message if processing failed
                    # SQS will automatically move it to DLQ after max receive count
                    # or make it available for retry after visibility timeout

                    # Log if this message is approaching the DLQ threshold
                    if receive_count >= Config.SQS_MAX_RECEIVE_COUNT - 1:
                        logger.error(
                            f"Message will be moved to DLQ on next failure (receive count: {receive_count})"
                        )

                    continue

            if processed_count > 0:
                logger.info(f"Successfully processed {processed_count} messages")

            return processed_count

        except Exception as e:
            logger.error(f"Error processing messages: {e}")
            return 0

    async def run(self):
        """Main processing loop"""
        logger.info("Starting SQS Message Processor")

        # Wait for Redis to be available
        if not await self._wait_for_redis_connection():
            logger.error("Could not connect to Redis, exiting")
            sys.exit(1)

        # Get queue URL and setup DLQ
        await self._get_queue_url()

        # Log DLQ configuration for monitoring
        logger.info(f"Main queue: {Config.SQS_QUEUE_NAME}")
        logger.info(f"Dead letter queue: {Config.DLQ_QUEUE_NAME}")
        logger.info(f"Visibility timeout: {Config.SQS_VISIBILITY_TIMEOUT} seconds")
        logger.info(f"Max receive count: {Config.SQS_MAX_RECEIVE_COUNT}")

        logger.info("SQS Message Processor started successfully")

        while self.running:
            try:
                processed_count = self.process_messages()

                # Sleep only if no messages were processed
                if processed_count == 0:
                    await asyncio.sleep(Config.PROCESSOR_SLEEP_INTERVAL)

            except Exception as e:
                logger.error(f"Error in main processing loop: {e}")
                await asyncio.sleep(Config.PROCESSOR_SLEEP_INTERVAL)

        logger.info("SQS Message Processor stopped")

    def get_dlq_message_count(self):
        """Get the approximate number of messages in the DLQ for monitoring"""
        try:
            if not self.dlq_url:
                return 0

            response = self.sqs_client.get_queue_attributes(
                QueueUrl=self.dlq_url, AttributeNames=["ApproximateNumberOfMessages"]
            )

            count = int(response["Attributes"]["ApproximateNumberOfMessages"])
            if count > 0:
                logger.warning(f"DLQ contains {count} messages that require attention")

            return count

        except Exception as e:
            logger.error(f"Failed to get DLQ message count: {e}")
            return 0


def main():
    """Entry point for the processor service"""
    import asyncio

    processor = SQSProcessor()

    try:
        asyncio.run(processor.run())
    except KeyboardInterrupt:
        logger.info("Processor stopped by user")
    except Exception as e:
        logger.error(f"Processor failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
