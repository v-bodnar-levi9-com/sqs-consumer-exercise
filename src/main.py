#!/usr/bin/env python
import asyncio
import collections
import json
import logging
import os
import localstack_client.session as boto3
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from pydantic import ValidationError

from .schemas import SQSMessageBody
from .process import process_messages

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger(__name__)

# Global state for event tracking
event_counts = collections.defaultdict(float)
event_sums = collections.defaultdict(float)
background_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting SQS consumer application")
    global background_task
    background_task = asyncio.create_task(message_processor())
    yield
    logger.info("Shutting down SQS consumer application")
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            logger.info("Background task cancelled successfully")


app = FastAPI(
    title="SQS Consumer API",
    description="FastAPI application for consuming SQS messages",
    version="0.1.0",
    lifespan=lifespan
)


class SQSConfig:
    def __init__(self):
        self.client = boto3.client('sqs')
        self.queue_name = os.getenv("SQS_QUEUE_NAME", "hands-on-interview")
        self.queue_url = None
    
    async def get_queue_url(self):
        if self.queue_url is None:
            try:
                logger.info(f"Getting queue URL for queue: {self.queue_name}")
                res = self.client.get_queue_url(QueueName=self.queue_name)
                self.queue_url = res["QueueUrl"]
                logger.info(f"Successfully retrieved queue URL: {self.queue_url}")
            except Exception:  # broad except kept; TODO narrow exception types
                logger.info(f"Queue {self.queue_name} not found, creating new queue")
                res = self.client.create_queue(QueueName=self.queue_name)
                self.queue_url = res["QueueUrl"]
                logger.info(f"Successfully created queue with URL: {self.queue_url}")
        return self.queue_url


sqs_config = SQSConfig()
def get_sqs_config() -> SQSConfig:
    return sqs_config


async def message_processor():
    """Background task to continuously process SQS messages"""
    global event_counts, event_sums
    
    logger.info("Starting background message processor")
    queue_url = await sqs_config.get_queue_url()
    
    while True:
        try:
            logger.info("Fetching messages from queue...")
            # Run the blocking operation in a thread pool
            await asyncio.get_event_loop().run_in_executor(
                None, 
                process_messages, 
                queue_url, 
                event_counts, 
                event_sums
            )
            print_stats(event_counts, event_sums)
            
        except Exception as e:
            logger.error(f"Error in message processor: {e}")
            await asyncio.sleep(5)


def print_stats(event_counts, event_sums):
    for key in event_counts:
        logger.info(f'Event type: {key}')
        logger.info(f'Count: {event_counts[key]}')
        logger.info(f'Sum: {event_sums[key]}')


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
