import asyncio
import os
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def test_client():
    """Create a test client for FastAPI app"""
    from src.api.main import app

    return TestClient(app)


@pytest.fixture
def mock_sqs_client():
    """Create a mock SQS client"""
    with patch("localstack_client.session.client") as mock_client:
        mock_sqs = Mock()
        mock_client.return_value = mock_sqs
        yield mock_sqs


@pytest.fixture
def mock_redis_client():
    """Create a mock Redis client"""
    with patch("src.shared.redis_client.redis.Redis") as mock_redis_class:
        mock_redis = Mock()
        mock_redis_class.return_value = mock_redis
        yield mock_redis


@pytest.fixture
def mock_stats_service():
    """Create a mock stats service"""
    with patch("src.api.stats.stats_service") as mock_service:
        yield mock_service


@pytest.fixture
def mock_environment():
    """Mock environment variables"""
    original_env = os.environ.copy()
    test_env = {
        "SQS_QUEUE_NAME": "test-queue",
        "REDIS_HOST": "test-redis",
        "REDIS_PORT": "6380",
        "LOG_LEVEL": "DEBUG",
    }

    # Set test environment variables
    for key, value in test_env.items():
        os.environ[key] = value

    yield test_env

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_processor():
    """Create a mock SQS processor"""
    with patch("src.processor.main.SQSProcessor") as mock_processor_class:
        mock_instance = Mock()
        mock_processor_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_sqs_messages():
    """Provide sample SQS messages for testing"""
    return [
        {
            "Body": '{"type": "user_signup", "value": 42}',
            "ReceiptHandle": "test-handle-1",
        },
        {
            "Body": '{"type": "user_login", "value": 10.5}',
            "ReceiptHandle": "test-handle-2",
        },
        {"Body": '{"type": "page_view", "value": 1}', "ReceiptHandle": "test-handle-3"},
    ]


@pytest.fixture
def sample_event_stats():
    """Provide sample event statistics for testing"""
    from src.shared.schemas import EventStats

    return {
        "user_signup": EventStats(count=10.0, total=250.0),
        "user_login": EventStats(count=5.0, total=75.0),
        "page_view": EventStats(count=100.0, total=100.0),
    }


@pytest.fixture
def sample_stats_response():
    """Provide sample stats response data for testing"""
    from src.shared.schemas import StatsResponse

    return [
        StatsResponse(event_type="user_signup", count=10.0, total=250.0, average=25.0),
        StatsResponse(event_type="user_login", count=5.0, total=75.0, average=15.0),
    ]


@pytest.fixture
def clean_global_state():
    """Reset global state for each test"""
    # Since we moved to a microservices architecture with Redis,
    # this fixture is no longer needed but kept for compatibility
    yield


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for async tests"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def api_test_client():
    """Create a test client specifically for the API app"""
    from src.api.main import app as api_app

    return TestClient(api_app)


@pytest.fixture
def mock_config():
    """Mock the shared configuration"""
    with patch("src.shared.config.Config") as mock_config_class:
        mock_config_class.AWS_ENDPOINT_URL = "http://test-localstack:4566"
        mock_config_class.SQS_QUEUE_NAME = "test-queue"
        mock_config_class.REDIS_HOST = "test-redis"
        mock_config_class.REDIS_PORT = 6379
        mock_config_class.REDIS_DB = 0
        mock_config_class.MAX_MESSAGES_PER_BATCH = 10
        mock_config_class.SQS_WAIT_TIME_SECONDS = 20
        mock_config_class.PROCESSOR_SLEEP_INTERVAL = 1
        mock_config_class.API_HOST = "0.0.0.0"
        mock_config_class.API_PORT = 8000
        mock_config_class.LOG_LEVEL = "INFO"
        yield mock_config_class


@pytest.fixture
def sample_sqs_message_data():
    """Sample SQS message data for testing"""
    return {"type": "user_signup", "value": 42}


# Mark all tests as unit tests by default
def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line("markers", "unit: mark test as a unit test")


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests"""
    for item in items:
        # Add unit marker to all tests unless they have integration marker
        if not any(marker.name == "integration" for marker in item.iter_markers()):
            item.add_marker(pytest.mark.unit)
