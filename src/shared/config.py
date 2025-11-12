import os
from typing import Optional


class Config:
    """Shared configuration for all services"""

    AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "hands-on-interview")
    SQS_VISIBILITY_TIMEOUT = int(
        os.getenv("SQS_VISIBILITY_TIMEOUT", "300")
    )  # 5 minutes
    SQS_MAX_RECEIVE_COUNT = int(os.getenv("SQS_MAX_RECEIVE_COUNT", "3"))
    DLQ_QUEUE_NAME = os.getenv(
        "DLQ_QUEUE_NAME", f"{os.getenv('SQS_QUEUE_NAME', 'hands-on-interview')}-dlq"
    )

    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))

    MAX_MESSAGES_PER_BATCH = int(os.getenv("MAX_MESSAGES_PER_BATCH", "10"))
    SQS_WAIT_TIME_SECONDS = int(os.getenv("SQS_WAIT_TIME_SECONDS", "20"))
    PROCESSOR_SLEEP_INTERVAL = int(os.getenv("PROCESSOR_SLEEP_INTERVAL", "1"))

    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


REDIS_COUNT_KEY = "stats:count:{event_type}"
REDIS_SUM_KEY = "stats:sum:{event_type}"
REDIS_EVENTS_SET = "stats:event_types"
