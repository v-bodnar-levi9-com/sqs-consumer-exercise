import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from src.main import app, SQSConfig, get_sqs_config, lifespan, event_counts, event_sums


class TestSQSConfig:
    """Test cases for SQSConfig class"""
    
    def setup_method(self):
        """Reset the config for each test"""
        self.config = SQSConfig()
    
    @patch('localstack_client.session.client')
    def test_sqs_config_initialization(self, mock_client):
        """Test SQSConfig initialization"""
        config = SQSConfig()
        
        assert config.queue_name == "hands-on-interview"  # default value
        assert config.queue_url is None
        mock_client.assert_called_once_with('sqs')
    
    @patch.dict('os.environ', {'SQS_QUEUE_NAME': 'test-queue'})
    @patch('localstack_client.session.client')
    def test_sqs_config_custom_queue_name(self, mock_client):
        """Test SQSConfig with custom queue name from environment"""
        config = SQSConfig()
        
        assert config.queue_name == "test-queue"
        assert config.queue_url is None
    
    @patch('localstack_client.session.client')
    @pytest.mark.asyncio
    async def test_get_queue_url_existing_queue(self, mock_client):
        """Test getting queue URL for existing queue"""
        # Mock SQS client
        mock_sqs_client = Mock()
        mock_sqs_client.get_queue_url.return_value = {"QueueUrl": "http://localhost:4566/000000000000/test-queue"}
        mock_client.return_value = mock_sqs_client
        
        config = SQSConfig()
        queue_url = await config.get_queue_url()
        
        assert queue_url == "http://localhost:4566/000000000000/test-queue"
        assert config.queue_url == "http://localhost:4566/000000000000/test-queue"
        mock_sqs_client.get_queue_url.assert_called_once_with(QueueName=config.queue_name)
    
    @patch('localstack_client.session.client')
    @pytest.mark.asyncio
    async def test_get_queue_url_cached(self, mock_client):
        """Test that queue URL is cached after first call"""
        # Mock SQS client
        mock_sqs_client = Mock()
        mock_sqs_client.get_queue_url.return_value = {"QueueUrl": "http://localhost:4566/000000000000/test-queue"}
        mock_client.return_value = mock_sqs_client
        
        config = SQSConfig()
        
        # First call
        queue_url1 = await config.get_queue_url()
        # Second call
        queue_url2 = await config.get_queue_url()
        
        assert queue_url1 == queue_url2
        # Should only be called once due to caching
        mock_sqs_client.get_queue_url.assert_called_once()
    
    @patch('localstack_client.session.client')
    @pytest.mark.asyncio
    async def test_get_queue_url_create_new_queue(self, mock_client):
        """Test creating new queue when it doesn't exist"""
        # Mock SQS client
        mock_sqs_client = Mock()
        mock_sqs_client.get_queue_url.side_effect = Exception("Queue not found")
        mock_sqs_client.create_queue.return_value = {"QueueUrl": "http://localhost:4566/000000000000/new-queue"}
        mock_client.return_value = mock_sqs_client
        
        config = SQSConfig()
        queue_url = await config.get_queue_url()
        
        assert queue_url == "http://localhost:4566/000000000000/new-queue"
        mock_sqs_client.get_queue_url.assert_called_once_with(QueueName=config.queue_name)
        mock_sqs_client.create_queue.assert_called_once_with(QueueName=config.queue_name)


class TestDependencyInjection:
    """Test cases for FastAPI dependency injection"""
    
    def test_get_sqs_config_dependency(self):
        """Test the SQS config dependency function"""
        config = get_sqs_config()
        
        assert isinstance(config, SQSConfig)
        assert config.queue_name == "hands-on-interview"


class TestLifespan:
    """Test cases for FastAPI lifespan events"""
    
    def test_lifespan_function_exists(self):
        """Test that lifespan function is properly defined"""
        from src.main import lifespan
        import inspect
        
        # Verify it's an function (async context manager)
        assert inspect.isfunction(lifespan)
        # Check that it has the contextmanager decorator
        assert hasattr(lifespan, '__wrapped__')


class TestUtilityFunctions:
    """Test cases for utility functions"""
    
    @patch('src.main.logger')
    def test_print_stats(self, mock_logger):
        """Test print_stats function"""
        from src.main import print_stats
        import collections
        
        test_event_counts = collections.defaultdict(float)
        test_event_sums = collections.defaultdict(float)
        
        test_event_counts['user_signup'] = 5.0
        test_event_counts['user_login'] = 10.0
        test_event_sums['user_signup'] = 100.0
        test_event_sums['user_login'] = 250.0
        
        print_stats(test_event_counts, test_event_sums)
        
        # Check that logger.info was called with correct messages
        expected_calls = [
            (('Event type: user_signup',),),
            (('Count: 5.0',),),
            (('Sum: 100.0',),),
            (('Event type: user_login',),),
            (('Count: 10.0',),),
            (('Sum: 250.0',),)
        ]
        
        assert mock_logger.info.call_count == 6
        actual_calls = mock_logger.info.call_args_list
        
        for expected, actual in zip(expected_calls, actual_calls):
            assert expected == actual


class TestMessageProcessor:
    """Test cases for message processor background task"""
    
    @patch('src.main.process_messages')
    @patch('src.main.print_stats')
    @patch('src.main.sqs_config')
    @pytest.mark.asyncio
    async def test_message_processor_success(self, mock_sqs_config, mock_print_stats, mock_process_messages):
        """Test successful message processing iteration"""
        from src.main import message_processor
        import collections
        
        # Create an async mock for get_queue_url
        async def mock_get_queue_url():
            return "http://localhost:4566/000000000000/test-queue"
        
        mock_sqs_config.get_queue_url = mock_get_queue_url
        
        # Mock process_messages to avoid blocking
        mock_process_messages.return_value = None
        
        # Create the task and cancel it after one iteration
        task = asyncio.create_task(message_processor())
        
        # Give it a moment to start
        await asyncio.sleep(0.1)
        
        # Cancel the task
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected
        
        # Verify that the methods were called
        mock_process_messages.assert_called()
        mock_print_stats.assert_called()
    
    @patch('src.main.process_messages')
    @patch('src.main.logger')
    @patch('src.main.sqs_config')
    @pytest.mark.asyncio
    async def test_message_processor_error_handling(self, mock_sqs_config, mock_logger, mock_process_messages):
        """Test error handling in message processor"""
        from src.main import message_processor
        
        # Create an async mock for get_queue_url
        async def mock_get_queue_url():
            return "http://localhost:4566/000000000000/test-queue"
        
        mock_sqs_config.get_queue_url = mock_get_queue_url
        
        # Mock process_messages to raise an exception
        mock_process_messages.side_effect = Exception("SQS error")
        
        # Create the task and cancel it after one iteration
        task = asyncio.create_task(message_processor())
        
        # Give it a moment to start and hit the error
        await asyncio.sleep(0.1)
        
        # Cancel the task
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected
        
        # Verify that error was logged
        mock_logger.error.assert_called()
        error_call_args = mock_logger.error.call_args[0][0]
        assert "Error in message processor" in error_call_args
