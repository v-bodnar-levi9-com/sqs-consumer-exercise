import json
import logging
import signal
import sys
import time
import localstack_client.session as boto3
from pydantic import ValidationError

from ..shared.config import Config
from ..shared.schemas import SQSMessageBody
from ..shared.redis_client import redis_client

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

        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)

    def _shutdown_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    async def _get_queue_url(self):
        """Get or create SQS queue URL"""
        if self.queue_url is None:
            try:
                logger.info(f"Getting queue URL for queue: {Config.SQS_QUEUE_NAME}")
                res = self.sqs_client.get_queue_url(QueueName=Config.SQS_QUEUE_NAME)
                self.queue_url = res["QueueUrl"]
                logger.info(f"Successfully retrieved queue URL: {self.queue_url}")
            except Exception:
                logger.info(
                    f"Queue {Config.SQS_QUEUE_NAME} not found, creating new queue"
                )
                res = self.sqs_client.create_queue(QueueName=Config.SQS_QUEUE_NAME)
                self.queue_url = res["QueueUrl"]
                logger.info(f"Successfully created queue with URL: {self.queue_url}")
        return self.queue_url

    def _wait_for_redis(self, max_retries: int = 30):
        """Wait for Redis to be available"""
        for attempt in range(max_retries):
            try:
                if redis_client.ping():
                    logger.info("Successfully connected to Redis")
                    return True
            except Exception as e:
                logger.warning(
                    f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}"
                )
                time.sleep(1)

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
                    # Update Redis stats
                    redis_client.increment_event(
                        message_data.type, float(message_data.value)
                    )

                    # Delete message from SQS
                    self.sqs_client.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=receipt_handle
                    )

                    processed_count += 1
                    logger.debug(
                        f"Processed message: type={message_data.type}, value={message_data.value}"
                    )

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Don't delete the message if processing failed
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
        if not self._wait_for_redis():
            logger.error("Could not connect to Redis, exiting")
            sys.exit(1)

        # Get queue URL
        await self._get_queue_url()

        logger.info("SQS Message Processor started successfully")

        while self.running:
            try:
                processed_count = self.process_messages()

                # Sleep only if no messages were processed
                if processed_count == 0:
                    time.sleep(Config.PROCESSOR_SLEEP_INTERVAL)

            except Exception as e:
                logger.error(f"Error in main processing loop: {e}")
                time.sleep(Config.PROCESSOR_SLEEP_INTERVAL)

        logger.info("SQS Message Processor stopped")


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
