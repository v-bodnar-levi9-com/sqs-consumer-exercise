import pytest
import asyncio
import os
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from src.main import app, SQSConfig


@pytest.fixture
def test_client():
    """Create a test client for FastAPI app"""
    return TestClient(app)


@pytest.fixture
def mock_sqs_client():
    """Create a mock SQS client"""
    with patch('localstack_client.session.client') as mock_client:
        mock_sqs = Mock()
        mock_client.return_value = mock_sqs
        yield mock_sqs


@pytest.fixture
def sqs_config():
    """Create a fresh SQSConfig instance for testing"""
    return SQSConfig()


@pytest.fixture
def mock_environment():
    """Mock environment variables"""
    original_env = os.environ.copy()
    test_env = {
        'SQS_QUEUE_NAME': 'test-queue'
    }
    
    # Set test environment variables
    for key, value in test_env.items():
        os.environ[key] = value
    
    yield test_env
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_sqs_message_data():
    """Sample SQS message data for testing"""
    return {
        "type": "user_signup",
        "value": 42
    }


@pytest.fixture
def sample_sqs_messages():
    """Sample collection of SQS messages for testing"""
    return [
        {"type": "user_signup", "value": 1},
        {"type": "user_login", "value": 2},
        {"type": "user_rating", "value": 4.5},
        {"type": "purchase", "value": 99.99},
    ]


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
