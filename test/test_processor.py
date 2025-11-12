import pytest
import json
import signal
import time
import asyncio
from unittest.mock import Mock, patch, AsyncMock, call
from pydantic import ValidationError

from src.processor.main import SQSProcessor, main
from src.shared.schemas import SQSMessageBody


class TestSQSProcessor:
    """Test cases for the SQSProcessor class"""

    def setup_method(self):
        """Setup a fresh SQSProcessor for each test"""
        self.processor = SQSProcessor()

    @patch("src.processor.main.boto3.client")
    def test_processor_initialization(self, mock_boto3_client):
        """Test SQSProcessor initialization"""
        mock_sqs_client = Mock()
        mock_boto3_client.return_value = mock_sqs_client

        processor = SQSProcessor()

        assert processor.running is True
        assert processor.sqs_client == mock_sqs_client
        assert processor.queue_url is None
        mock_boto3_client.assert_called_once_with("sqs")

    def test_shutdown_handler(self):
        """Test graceful shutdown signal handler"""
        processor = SQSProcessor()
        assert processor.running is True

        # Simulate receiving a shutdown signal
        processor._shutdown_handler(signal.SIGINT, None)

        assert processor.running is False

    @patch("src.processor.main.logger")
    def test_shutdown_handler_logging(self, mock_logger):
        """Test that shutdown handler logs the signal"""
        processor = SQSProcessor()

        processor._shutdown_handler(signal.SIGTERM, None)

        mock_logger.info.assert_called_once()
        log_message = mock_logger.info.call_args[0][0]
        assert "Received signal 15" in log_message  # SIGTERM = 15
        assert "shutting down gracefully" in log_message

    @pytest.mark.asyncio
    async def test_get_queue_url_existing_queue(self):
        """Test getting queue URL for existing queue"""
        self.processor.sqs_client = Mock()
        self.processor.sqs_client.get_queue_url.return_value = {
            "QueueUrl": "http://localhost:4566/000000000000/test-queue"
        }

        queue_url = await self.processor._get_queue_url()

        assert queue_url == "http://localhost:4566/000000000000/test-queue"
        assert (
            self.processor.queue_url == "http://localhost:4566/000000000000/test-queue"
        )

        self.processor.sqs_client.get_queue_url.assert_called_once_with(
            QueueName="hands-on-interview"  # default queue name
        )

    @pytest.mark.asyncio
    async def test_get_queue_url_create_new_queue(self):
        """Test creating new queue when it doesn't exist"""
        self.processor.sqs_client = Mock()
        self.processor.sqs_client.get_queue_url.side_effect = Exception(
            "Queue not found"
        )
        self.processor.sqs_client.create_queue.return_value = {
            "QueueUrl": "http://localhost:4566/000000000000/new-queue"
        }

        queue_url = await self.processor._get_queue_url()

        assert queue_url == "http://localhost:4566/000000000000/new-queue"
        assert (
            self.processor.queue_url == "http://localhost:4566/000000000000/new-queue"
        )

        self.processor.sqs_client.create_queue.assert_called_once_with(
            QueueName="hands-on-interview"
        )

    @pytest.mark.asyncio
    async def test_get_queue_url_caching(self):
        """Test that queue URL is cached after first call"""
        self.processor.sqs_client = Mock()
        self.processor.sqs_client.get_queue_url.return_value = {
            "QueueUrl": "http://localhost:4566/000000000000/cached-queue"
        }

        # First call
        queue_url1 = await self.processor._get_queue_url()
        # Second call
        queue_url2 = await self.processor._get_queue_url()

        assert queue_url1 == queue_url2
        # Should only be called once due to caching
        self.processor.sqs_client.get_queue_url.assert_called_once()

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    def test_wait_for_redis_success(self, mock_sleep, mock_redis_client):
        """Test waiting for Redis when connection succeeds immediately"""
        mock_redis_client.ping.return_value = True

        result = self.processor._wait_for_redis(max_retries=5)

        assert result is True
        mock_redis_client.ping.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    @patch("src.processor.main.logger")
    def test_wait_for_redis_retry_then_success(
        self, mock_logger, mock_sleep, mock_redis_client
    ):
        """Test waiting for Redis with retries before success"""
        # First two calls fail, third succeeds
        mock_redis_client.ping.side_effect = [
            Exception("Connection failed"),
            Exception("Connection failed"),
            True,
        ]

        result = self.processor._wait_for_redis(max_retries=5)

        assert result is True
        assert mock_redis_client.ping.call_count == 3
        assert mock_sleep.call_count == 2

        # Verify warning logs for failed attempts
        assert mock_logger.warning.call_count == 2
        mock_logger.info.assert_called_once_with("Successfully connected to Redis")

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    @patch("src.processor.main.logger")
    def test_wait_for_redis_max_retries_exceeded(
        self, mock_logger, mock_sleep, mock_redis_client
    ):
        """Test waiting for Redis when max retries are exceeded"""
        mock_redis_client.ping.side_effect = Exception("Connection failed")

        result = self.processor._wait_for_redis(max_retries=3)

        assert result is False
        assert mock_redis_client.ping.call_count == 3
        assert mock_sleep.call_count == 3

        mock_logger.error.assert_called_once_with(
            "Failed to connect to Redis after maximum retries"
        )

    @patch("src.processor.main.redis_client")
    def test_process_messages_no_messages(self, mock_redis_client):
        """Test processing when no messages are received"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()
        self.processor.sqs_client.receive_message.return_value = {}

        result = self.processor.process_messages()

        assert result == 0
        self.processor.sqs_client.receive_message.assert_called_once_with(
            QueueUrl="test-queue-url",
            MaxNumberOfMessages=10,  # from Config.MAX_MESSAGES_PER_BATCH
            WaitTimeSeconds=20,  # from Config.SQS_WAIT_TIME_SECONDS
        )

    @patch("src.processor.main.redis_client")
    def test_process_messages_valid_single_message(self, mock_redis_client):
        """Test processing a single valid message"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        message_body = {"type": "user_signup", "value": 42}
        self.processor.sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle",
                }
            ]
        }

        result = self.processor.process_messages()

        assert result == 1

        # Verify Redis was updated
        mock_redis_client.increment_event.assert_called_once_with("user_signup", 42.0)

        # Verify message was deleted
        self.processor.sqs_client.delete_message.assert_called_once_with(
            QueueUrl="test-queue-url", ReceiptHandle="test-receipt-handle"
        )

    @patch("src.processor.main.redis_client")
    def test_process_messages_multiple_valid_messages(self, mock_redis_client):
        """Test processing multiple valid messages"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        messages = [
            {
                "Body": json.dumps({"type": "user_signup", "value": 10}),
                "ReceiptHandle": "handle1",
            },
            {
                "Body": json.dumps({"type": "user_login", "value": 5}),
                "ReceiptHandle": "handle2",
            },
            {
                "Body": json.dumps({"type": "user_signup", "value": 15}),
                "ReceiptHandle": "handle3",
            },
        ]

        self.processor.sqs_client.receive_message.return_value = {"Messages": messages}

        result = self.processor.process_messages()

        assert result == 3

        # Verify Redis increments
        expected_calls = [
            call("user_signup", 10.0),
            call("user_login", 5.0),
            call("user_signup", 15.0),
        ]
        mock_redis_client.increment_event.assert_has_calls(expected_calls)

        # Verify all messages were deleted
        expected_delete_calls = [
            call(QueueUrl="test-queue-url", ReceiptHandle="handle1"),
            call(QueueUrl="test-queue-url", ReceiptHandle="handle2"),
            call(QueueUrl="test-queue-url", ReceiptHandle="handle3"),
        ]
        self.processor.sqs_client.delete_message.assert_has_calls(expected_delete_calls)

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.logger")
    def test_process_messages_invalid_json(self, mock_logger, mock_redis_client):
        """Test processing messages with invalid JSON"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        messages = [
            {"Body": "invalid-json", "ReceiptHandle": "handle1"},
            {
                "Body": json.dumps({"type": "user_signup", "value": 10}),
                "ReceiptHandle": "handle2",
            },
        ]

        self.processor.sqs_client.receive_message.return_value = {"Messages": messages}

        result = self.processor.process_messages()

        assert result == 1  # Only one valid message processed

        # Verify warning logged for invalid JSON
        mock_logger.warning.assert_called()
        warning_message = mock_logger.warning.call_args_list[0][0][0]
        assert "invalid JSON" in warning_message

        # Verify both messages were deleted
        assert self.processor.sqs_client.delete_message.call_count == 2

        # Verify only valid message updated Redis
        mock_redis_client.increment_event.assert_called_once_with("user_signup", 10.0)

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.logger")
    def test_process_messages_invalid_schema(self, mock_logger, mock_redis_client):
        """Test processing messages with invalid schema"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        messages = [
            {
                "Body": json.dumps({"type": "user_signup"}),  # missing value
                "ReceiptHandle": "handle1",
            },
            {
                "Body": json.dumps({"value": 10}),  # missing type
                "ReceiptHandle": "handle2",
            },
            {
                "Body": json.dumps({"type": "user_login", "value": 5}),  # valid
                "ReceiptHandle": "handle3",
            },
        ]

        self.processor.sqs_client.receive_message.return_value = {"Messages": messages}

        result = self.processor.process_messages()

        assert result == 1  # Only one valid message processed

        # Verify warnings logged for invalid schema
        assert mock_logger.warning.call_count == 2

        # Verify all messages were deleted
        assert self.processor.sqs_client.delete_message.call_count == 3

        # Verify only valid message updated Redis
        mock_redis_client.increment_event.assert_called_once_with("user_login", 5.0)

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.logger")
    def test_process_messages_redis_error(self, mock_logger, mock_redis_client):
        """Test handling Redis errors during message processing"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        message = {
            "Body": json.dumps({"type": "user_signup", "value": 42}),
            "ReceiptHandle": "test-handle",
        }

        self.processor.sqs_client.receive_message.return_value = {"Messages": [message]}
        mock_redis_client.increment_event.side_effect = Exception("Redis error")

        result = self.processor.process_messages()

        assert result == 0  # No messages successfully processed

        # Verify error was logged
        mock_logger.error.assert_called()
        error_message = mock_logger.error.call_args[0][0]
        assert "Error processing message" in error_message

        # Message should NOT be deleted when processing fails
        self.processor.sqs_client.delete_message.assert_not_called()

    @patch("src.processor.main.redis_client")
    def test_process_messages_shutdown_during_processing(self, mock_redis_client):
        """Test that processing stops when shutdown is requested"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        messages = [
            {
                "Body": json.dumps({"type": "user_signup", "value": 10}),
                "ReceiptHandle": "handle1",
            },
            {
                "Body": json.dumps({"type": "user_login", "value": 5}),
                "ReceiptHandle": "handle2",
            },
        ]

        self.processor.sqs_client.receive_message.return_value = {"Messages": messages}

        # Set running to False after first message
        def stop_after_first_call(*args, **kwargs):
            self.processor.running = False

        mock_redis_client.increment_event.side_effect = stop_after_first_call

        result = self.processor.process_messages()

        # Should process only one message before stopping
        assert result == 1
        mock_redis_client.increment_event.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    @patch("src.processor.main.sys.exit")
    async def test_run_redis_connection_failure(
        self, mock_exit, mock_sleep, mock_redis_client
    ):
        """Test run method when Redis connection fails"""
        # Mock Redis connection failure
        with patch.object(self.processor, "_wait_for_redis", return_value=False):
            await self.processor.run()

        mock_exit.assert_called_once_with(1)

    @pytest.mark.asyncio
    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    async def test_run_successful_processing(self, mock_sleep, mock_redis_client):
        """Test run method with successful message processing"""
        # Mock successful setup
        with patch.object(
            self.processor, "_wait_for_redis", return_value=True
        ), patch.object(
            self.processor, "_get_queue_url", return_value="test-queue"
        ), patch.object(
            self.processor, "process_messages"
        ) as mock_process:

            # Mock process_messages to return different values then stop
            mock_process.side_effect = [5, 0, 3, 0]  # Process some messages, then none

            # Stop after a few iterations
            original_running = self.processor.running
            call_count = 0

            def side_effect():
                nonlocal call_count
                call_count += 1
                if call_count > 4:
                    self.processor.running = False
                return (
                    mock_process.side_effect[call_count - 1] if call_count <= 4 else 0
                )

            mock_process.side_effect = side_effect

            await self.processor.run()

        # Verify process_messages was called multiple times
        assert mock_process.call_count > 0

    @pytest.mark.asyncio
    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    @patch("src.processor.main.logger")
    async def test_run_processing_loop_error_handling(
        self, mock_logger, mock_sleep, mock_redis_client
    ):
        """Test error handling in the main processing loop"""
        with patch.object(
            self.processor, "_wait_for_redis", return_value=True
        ), patch.object(
            self.processor, "_get_queue_url", return_value="test-queue"
        ), patch.object(
            self.processor, "process_messages"
        ) as mock_process:

            # First call raises exception, second call succeeds, then stop
            mock_process.side_effect = [
                Exception("Processing error"),
                0,  # No messages processed, will cause sleep
            ]

            # Stop after two iterations
            call_count = 0
            original_side_effect = mock_process.side_effect

            def side_effect():
                nonlocal call_count
                call_count += 1
                if call_count > 2:
                    self.processor.running = False
                return (
                    original_side_effect[call_count - 1]
                    if call_count <= len(original_side_effect)
                    else 0
                )

            mock_process.side_effect = side_effect

            await self.processor.run()

        # Verify error was logged
        mock_logger.error.assert_called()
        error_message = mock_logger.error.call_args_list[-1][0][0]
        assert "Error in main processing loop" in error_message


class TestMainFunction:
    """Test cases for the main function"""

    @patch("src.processor.main.asyncio.run")
    @patch("src.processor.main.SQSProcessor")
    def test_main_function_success(self, mock_processor_class, mock_asyncio_run):
        """Test successful execution of main function"""
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor

        main()

        mock_processor_class.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_processor.run())

    @patch("src.processor.main.asyncio.run")
    @patch("src.processor.main.SQSProcessor")
    @patch("src.processor.main.logger")
    def test_main_function_keyboard_interrupt(
        self, mock_logger, mock_processor_class, mock_asyncio_run
    ):
        """Test main function handling KeyboardInterrupt"""
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        mock_asyncio_run.side_effect = KeyboardInterrupt()

        main()

        mock_logger.info.assert_called_once_with("Processor stopped by user")

    @patch("src.processor.main.asyncio.run")
    @patch("src.processor.main.SQSProcessor")
    @patch("src.processor.main.logger")
    @patch("src.processor.main.sys.exit")
    def test_main_function_exception(
        self, mock_exit, mock_logger, mock_processor_class, mock_asyncio_run
    ):
        """Test main function handling general exceptions"""
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        mock_asyncio_run.side_effect = Exception("Unexpected error")

        main()

        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert "Processor failed" in error_message
        mock_exit.assert_called_once_with(1)


class TestProcessorIntegration:
    """Integration tests for SQSProcessor"""

    def setup_method(self):
        """Setup processor for integration tests"""
        self.processor = SQSProcessor()

    @patch("src.processor.main.redis_client")
    def test_end_to_end_message_processing(self, mock_redis_client):
        """Test end-to-end message processing workflow"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        # Mix of valid, invalid JSON, and invalid schema messages
        messages = [
            {
                "Body": json.dumps({"type": "user_signup", "value": 25.5}),
                "ReceiptHandle": "handle1",
            },
            {"Body": "invalid-json", "ReceiptHandle": "handle2"},
            {
                "Body": json.dumps({"type": "user_login", "value": 10}),
                "ReceiptHandle": "handle3",
            },
            {"Body": json.dumps({"invalid": "schema"}), "ReceiptHandle": "handle4"},
            {
                "Body": json.dumps({"type": "page_view", "value": 1}),
                "ReceiptHandle": "handle5",
            },
        ]

        self.processor.sqs_client.receive_message.return_value = {"Messages": messages}

        result = self.processor.process_messages()

        # Should successfully process 3 valid messages
        assert result == 3

        # Verify Redis was updated for each valid message
        expected_calls = [
            call("user_signup", 25.5),
            call("user_login", 10.0),
            call("page_view", 1.0),
        ]
        mock_redis_client.increment_event.assert_has_calls(expected_calls)

        # Verify all messages were deleted (even invalid ones)
        assert self.processor.sqs_client.delete_message.call_count == 5

    @patch("src.processor.main.redis_client")
    def test_large_batch_processing(self, mock_redis_client):
        """Test processing a large batch of messages"""
        self.processor.queue_url = "test-queue-url"
        self.processor.sqs_client = Mock()

        # Generate 100 valid messages
        messages = []
        for i in range(100):
            messages.append(
                {
                    "Body": json.dumps({"type": f"event_type_{i % 5}", "value": i}),
                    "ReceiptHandle": f"handle_{i}",
                }
            )

        self.processor.sqs_client.receive_message.return_value = {"Messages": messages}

        result = self.processor.process_messages()

        assert result == 100
        assert mock_redis_client.increment_event.call_count == 100
        assert self.processor.sqs_client.delete_message.call_count == 100
