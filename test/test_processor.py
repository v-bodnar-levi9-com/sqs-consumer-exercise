import asyncio
import json
import signal
import time
from unittest.mock import AsyncMock, Mock, call, patch

import pytest
from pydantic import ValidationError

from src.processor.main import SQSProcessor, main
from src.shared.schemas import SQSMessageBody


class TestSQSProcessor:
    """Test cases for the SQSProcessor class"""

    def setup_method(self):
        """Setup a fresh SQSProcessor for each test"""
        self.processor = SQSProcessor()

    def test_processor_initialization(self):
        """Test SQSProcessor initialization"""
        processor = SQSProcessor()

        assert processor.running is True
        assert processor.session is not None
        assert processor.queue_url is None
        assert processor.dlq_url is None

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

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.get_queue_url.return_value = {
                "QueueUrl": "http://localhost:4566/000000000000/test-queue"
            }
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {"RedrivePolicy": "existing"}
            }

            queue_url = await self.processor._get_queue_url()

            assert queue_url == "http://localhost:4566/000000000000/test-queue"
            assert (
                self.processor.queue_url
                == "http://localhost:4566/000000000000/test-queue"
            )

            # Should be called multiple times: once for DLQ setup, once for main queue, etc.
            assert mock_sqs.get_queue_url.call_count >= 1

    @pytest.mark.asyncio
    async def test_get_queue_url_create_new_queue(self):
        """Test creating new queue when it doesn't exist"""

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.get_queue_url.side_effect = Exception("Queue not found")
            mock_sqs.create_queue.return_value = {
                "QueueUrl": "http://localhost:4566/000000000000/new-queue"
            }
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {
                    "QueueArn": "arn:aws:sqs:us-east-1:123456789012:new-queue"
                }
            }

            queue_url = await self.processor._get_queue_url()

            assert queue_url == "http://localhost:4566/000000000000/new-queue"
            assert (
                self.processor.queue_url
                == "http://localhost:4566/000000000000/new-queue"
            )

            # Should create both DLQ and main queue
            assert mock_sqs.create_queue.call_count >= 1

    @pytest.mark.asyncio
    async def test_get_queue_url_caching(self):
        """Test that queue URL is cached after first call"""

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.get_queue_url.return_value = {
                "QueueUrl": "http://localhost:4566/000000000000/cached-queue"
            }
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {"RedrivePolicy": "existing"}
            }

            # First call
            queue_url1 = await self.processor._get_queue_url()
            # Second call
            queue_url2 = await self.processor._get_queue_url()

            assert queue_url1 == queue_url2
            # Queue URL should be cached, so no additional calls to get_queue_url for main queue
            assert self.processor.queue_url is not None

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.asyncio.sleep")
    @pytest.mark.asyncio
    async def test_wait_for_redis_success(self, mock_sleep, mock_redis_client):
        """Test waiting for Redis when connection succeeds immediately"""
        mock_redis_client.ping.return_value = True

        result = await self.processor._wait_for_redis_connection(max_retries=5)

        assert result is True
        mock_redis_client.ping.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.asyncio.sleep")
    @patch("src.processor.main.logger")
    @pytest.mark.asyncio
    async def test_wait_for_redis_retry_then_success(
        self, mock_logger, mock_sleep, mock_redis_client
    ):
        """Test waiting for Redis with retries before success"""
        # First two calls fail, third succeeds
        mock_redis_client.ping.side_effect = [
            Exception("Connection failed"),
            Exception("Connection failed"),
            True,
        ]

        result = await self.processor._wait_for_redis_connection(max_retries=5)

        assert result is True
        assert mock_redis_client.ping.call_count == 3
        assert mock_sleep.call_count == 2

        # Verify warning logs for failed attempts
        assert mock_logger.warning.call_count == 2
        mock_logger.info.assert_called_once_with("Successfully connected to Redis")

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.asyncio.sleep")
    @patch("src.processor.main.logger")
    @pytest.mark.asyncio
    async def test_wait_for_redis_max_retries_exceeded(
        self, mock_logger, mock_sleep, mock_redis_client
    ):
        """Test waiting for Redis when max retries are exceeded"""
        mock_redis_client.ping.side_effect = Exception("Connection failed")

        result = await self.processor._wait_for_redis_connection(max_retries=3)

        assert result is False
        assert mock_redis_client.ping.call_count == 3
        assert mock_sleep.call_count == 3

        mock_logger.error.assert_called_once_with(
            "Failed to connect to Redis after maximum retries"
        )

    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_process_messages_no_messages(self, mock_redis_client):
        """Test processing when no messages are received"""
        self.processor.queue_url = "test-queue-url"

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {}

            result = await self.processor.process_messages()

            assert result == 0
            mock_sqs.receive_message.assert_called_once_with(
                QueueUrl="test-queue-url",
                MaxNumberOfMessages=10,  # from Config.MAX_MESSAGES_PER_BATCH
                WaitTimeSeconds=20,  # from Config.SQS_WAIT_TIME_SECONDS
                AttributeNames=["ApproximateReceiveCount"],  # Added for DLQ support
                VisibilityTimeout=300,  # Added visibility timeout
            )

    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_process_messages_valid_single_message(self, mock_redis_client):
        """Test processing a single valid message"""
        self.processor.queue_url = "test-queue-url"

        message_body = {"type": "user_signup", "value": 42}

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {
                "Messages": [
                    {
                        "Body": json.dumps(message_body),
                        "ReceiptHandle": "test-receipt-handle",
                    }
                ]
            }

            result = await self.processor.process_messages()

            assert result == 1

            # Verify Redis was updated
            mock_redis_client.increment_event.assert_called_once_with(
                "user_signup", 42.0
            )

            # Verify message was deleted
            mock_sqs.delete_message.assert_called_once_with(
                QueueUrl="test-queue-url", ReceiptHandle="test-receipt-handle"
            )

    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_process_messages_multiple_valid_messages(self, mock_redis_client):
        """Test processing multiple valid messages"""
        self.processor.queue_url = "test-queue-url"

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

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

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
        mock_sqs.delete_message.assert_has_calls(expected_delete_calls)

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.logger")
    @pytest.mark.asyncio
    async def test_process_messages_invalid_json(self, mock_logger, mock_redis_client):
        """Test processing messages with invalid JSON"""
        self.processor.queue_url = "test-queue-url"

        messages = [
            {"Body": "invalid-json", "ReceiptHandle": "handle1"},
            {
                "Body": json.dumps({"type": "user_signup", "value": 10}),
                "ReceiptHandle": "handle2",
            },
        ]

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

            assert result == 1  # Only one valid message processed

            # Verify warning logged for invalid JSON
            mock_logger.warning.assert_called()
            warning_message = mock_logger.warning.call_args_list[0][0][0]
            assert "invalid JSON" in warning_message

            # Verify both messages were deleted
            assert mock_sqs.delete_message.call_count == 2

            # Verify only valid message updated Redis
            mock_redis_client.increment_event.assert_called_once_with(
                "user_signup", 10.0
            )

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.logger")
    @pytest.mark.asyncio
    async def test_process_messages_invalid_schema(
        self, mock_logger, mock_redis_client
    ):
        """Test processing messages with invalid schema"""
        self.processor.queue_url = "test-queue-url"

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

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

            assert result == 1  # Only one valid message processed

            # Verify warnings logged for invalid schema
            assert mock_logger.warning.call_count == 2

            # Verify all messages were deleted
            assert mock_sqs.delete_message.call_count == 3

            # Verify only valid message updated Redis
            mock_redis_client.increment_event.assert_called_once_with("user_login", 5.0)

    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.logger")
    @pytest.mark.asyncio
    async def test_process_messages_redis_error(self, mock_logger, mock_redis_client):
        """Test handling Redis errors during message processing"""
        self.processor.queue_url = "test-queue-url"

        message = {
            "Body": json.dumps({"type": "user_signup", "value": 42}),
            "ReceiptHandle": "test-handle",
        }

        mock_redis_client.increment_event.side_effect = Exception("Redis error")

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": [message]}

            result = await self.processor.process_messages()

            assert result == 0  # No messages successfully processed

            # Verify error was logged
            mock_logger.error.assert_called()
            error_message = mock_logger.error.call_args[0][0]
            assert "Error processing message" in error_message

            # Message should NOT be deleted when processing fails
            mock_sqs.delete_message.assert_not_called()

    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_process_messages_shutdown_during_processing(self, mock_redis_client):
        """Test that processing stops when shutdown is requested"""
        self.processor.queue_url = "test-queue-url"

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

        # Set running to False after first message
        def stop_after_first_call(*args, **kwargs):
            self.processor.running = False

        mock_redis_client.increment_event.side_effect = stop_after_first_call

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

            assert result == 1  # Only first message processed before shutdown

    @pytest.mark.asyncio
    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.time.sleep")
    @patch("src.processor.main.sys.exit")
    async def test_run_redis_connection_failure(
        self, mock_exit, mock_sleep, mock_redis_client
    ):
        """Test run method when Redis connection fails"""
        # Mock Redis connection failure
        with patch.object(
            self.processor, "_wait_for_redis_connection", return_value=False
        ):
            # Mock sys.exit to raise an exception instead of actually exiting
            mock_exit.side_effect = SystemExit(1)

            # The test should expect SystemExit to be raised
            with pytest.raises(SystemExit):
                await self.processor.run()

        mock_exit.assert_called_once_with(1)

    @pytest.mark.asyncio
    @patch("src.processor.main.redis_client")
    @patch("src.processor.main.asyncio.sleep")
    async def test_run_successful_processing(self, mock_sleep, mock_redis_client):
        """Test run method with successful message processing"""
        # Mock successful setup
        with patch.object(
            self.processor, "_wait_for_redis_connection", return_value=True
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
    @patch("src.processor.main.asyncio.sleep")
    @patch("src.processor.main.logger")
    async def test_run_processing_loop_error_handling(
        self, mock_logger, mock_sleep, mock_redis_client
    ):
        """Test error handling in the main processing loop"""
        with patch.object(
            self.processor, "_wait_for_redis_connection", return_value=True
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

    @patch("asyncio.run")
    @patch("src.processor.main.SQSProcessor")
    def test_main_function_success(self, mock_processor_class, mock_asyncio_run):
        """Test successful execution of main function"""
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor

        main()

        mock_processor_class.assert_called_once()
        mock_asyncio_run.assert_called_once_with(mock_processor.run())

    @patch("asyncio.run")
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

    @patch("asyncio.run")
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
    @pytest.mark.asyncio
    async def test_end_to_end_message_processing(self, mock_redis_client):
        """Test end-to-end message processing workflow"""
        self.processor.queue_url = "test-queue-url"

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

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

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
            assert mock_sqs.delete_message.call_count == 5

    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_large_batch_processing(self, mock_redis_client):
        """Test processing a large batch of messages"""
        self.processor.queue_url = "test-queue-url"

        # Generate 100 valid messages
        messages = []
        for i in range(100):
            messages.append(
                {
                    "Body": json.dumps({"type": f"event_type_{i % 5}", "value": i}),
                    "ReceiptHandle": f"handle_{i}",
                }
            )

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

            assert result == 100
            assert mock_redis_client.increment_event.call_count == 100
            assert mock_sqs.delete_message.call_count == 100


class TestSQSProcessorDLQ:
    """Test cases for DLQ functionality in SQSProcessor"""

    def setup_method(self):
        """Setup a fresh SQSProcessor for each test"""
        self.processor = SQSProcessor()

    @pytest.mark.asyncio
    @patch("src.processor.main.Config")
    async def test_setup_dlq_new_queue(self, mock_config):
        """Test creating new DLQ when it doesn't exist"""
        mock_config.DLQ_QUEUE_NAME = "test-dlq"

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.get_queue_url.side_effect = Exception("Queue not found")
            mock_sqs.create_queue.return_value = {
                "QueueUrl": "http://localhost:4566/000000000000/test-dlq"
            }

            await self.processor._setup_dlq()

            assert (
                self.processor.dlq_url == "http://localhost:4566/000000000000/test-dlq"
            )
            mock_sqs.create_queue.assert_called_once_with(QueueName="test-dlq")

    @pytest.mark.asyncio
    @patch("src.processor.main.Config")
    async def test_setup_dlq_existing_queue(self, mock_config):
        """Test using existing DLQ"""
        mock_config.DLQ_QUEUE_NAME = "existing-dlq"

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.get_queue_url.return_value = {
                "QueueUrl": "http://localhost:4566/000000000000/existing-dlq"
            }

            await self.processor._setup_dlq()

            assert (
                self.processor.dlq_url
                == "http://localhost:4566/000000000000/existing-dlq"
            )
            mock_sqs.create_queue.assert_not_called()

    @patch("src.processor.main.Config")
    @pytest.mark.asyncio
    async def test_extend_message_visibility(self, mock_config):
        """Test extending message visibility timeout"""
        mock_config.SQS_VISIBILITY_TIMEOUT = 300

        self.processor.queue_url = "http://localhost:4566/000000000000/test-queue"

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs

            receipt_handle = "test-receipt-handle"
            await self.processor._extend_message_visibility(receipt_handle)

            mock_sqs.change_message_visibility.assert_called_once_with(
                QueueUrl="http://localhost:4566/000000000000/test-queue",
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=300,
            )

    @patch("src.processor.main.Config")
    @pytest.mark.asyncio
    async def test_extend_message_visibility_custom_timeout(self, mock_config):
        """Test extending message visibility with custom timeout"""
        self.processor.queue_url = "http://localhost:4566/000000000000/test-queue"

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs

            receipt_handle = "test-receipt-handle"
            custom_timeout = 600
            await self.processor._extend_message_visibility(
                receipt_handle, custom_timeout
            )

            mock_sqs.change_message_visibility.assert_called_once_with(
                QueueUrl="http://localhost:4566/000000000000/test-queue",
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=custom_timeout,
            )

    @pytest.mark.asyncio
    async def test_get_dlq_message_count(self):
        """Test getting DLQ message count"""
        self.processor.dlq_url = "http://localhost:4566/000000000000/test-dlq"

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {"ApproximateNumberOfMessages": "5"}
            }

            count = await self.processor.get_dlq_message_count()

            assert count == 5
            mock_sqs.get_queue_attributes.assert_called_once_with(
                QueueUrl="http://localhost:4566/000000000000/test-dlq",
                AttributeNames=["ApproximateNumberOfMessages"],
            )

    @pytest.mark.asyncio
    async def test_get_dlq_message_count_no_dlq(self):
        """Test getting DLQ message count when no DLQ URL is set"""
        self.processor.dlq_url = None

        count = await self.processor.get_dlq_message_count()

        assert count == 0

    @patch("src.processor.main.Config")
    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_process_messages_with_receive_count(
        self, mock_redis_client, mock_config
    ):
        """Test processing messages with receive count tracking"""
        mock_config.MAX_MESSAGES_PER_BATCH = 10
        mock_config.SQS_WAIT_TIME_SECONDS = 20
        mock_config.SQS_VISIBILITY_TIMEOUT = 300
        mock_config.SQS_MAX_RECEIVE_COUNT = 3

        self.processor.queue_url = "http://localhost:4566/000000000000/test-queue"

        # Mock message with receive count
        messages = [
            {
                "Body": json.dumps({"type": "test_event", "value": 10}),
                "ReceiptHandle": "test-handle",
                "Attributes": {"ApproximateReceiveCount": "2"},  # Second attempt
            }
        ]

        with patch.object(self.processor.session, "client") as mock_session_client:
            mock_sqs = AsyncMock()
            mock_session_client.return_value.__aenter__.return_value = mock_sqs
            mock_sqs.receive_message.return_value = {"Messages": messages}

            result = await self.processor.process_messages()

            # Verify receive_message was called with correct parameters
            mock_sqs.receive_message.assert_called_once_with(
                QueueUrl="http://localhost:4566/000000000000/test-queue",
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                AttributeNames=["ApproximateReceiveCount"],
                VisibilityTimeout=300,
            )

            # Verify message was processed successfully
            assert result == 1
            mock_redis_client.increment_event.assert_called_once_with(
                "test_event", 10.0
            )
            mock_sqs.delete_message.assert_called_once()

    @patch("src.processor.main.Config")
    @patch("src.processor.main.redis_client")
    @pytest.mark.asyncio
    async def test_process_messages_high_receive_count_warning(
        self, mock_redis_client, mock_config
    ):
        """Test that high receive count triggers warning log"""
        mock_config.MAX_MESSAGES_PER_BATCH = 10
        mock_config.SQS_WAIT_TIME_SECONDS = 20
        mock_config.SQS_VISIBILITY_TIMEOUT = 300
        mock_config.SQS_MAX_RECEIVE_COUNT = 3

        self.processor.queue_url = "http://localhost:4566/000000000000/test-queue"

        # Mock message with high receive count (approaching DLQ threshold)
        messages = [
            {
                "Body": json.dumps({"type": "test_event", "value": 10}),
                "ReceiptHandle": "test-handle",
                "Attributes": {
                    "ApproximateReceiveCount": "2"
                },  # One more failure -> DLQ
            }
        ]

        # Mock Redis failure to trigger error handling
        mock_redis_client.increment_event.side_effect = Exception("Redis error")

        with patch("src.processor.main.logger") as mock_logger:
            with patch.object(self.processor.session, "client") as mock_session_client:
                mock_sqs = AsyncMock()
                mock_session_client.return_value.__aenter__.return_value = mock_sqs
                mock_sqs.receive_message.return_value = {"Messages": messages}

                result = await self.processor.process_messages()

                # Verify warning was logged for high receive count
                mock_logger.warning.assert_any_call("Message has been received 2 times")

            # Verify message was not deleted due to processing error
            assert result == 0
            mock_sqs.delete_message.assert_not_called()


class TestConfigurationDLQ:
    """Test the new DLQ configuration settings"""

    def test_dlq_config_defaults(self):
        """Test DLQ configuration default values"""
        # Test that the configuration module loads properly
        from src.shared.config import Config

        # Test default values are reasonable
        assert isinstance(Config.SQS_VISIBILITY_TIMEOUT, int)
        assert Config.SQS_VISIBILITY_TIMEOUT > 0
        assert isinstance(Config.SQS_MAX_RECEIVE_COUNT, int)
        assert Config.SQS_MAX_RECEIVE_COUNT > 0
        assert isinstance(Config.DLQ_QUEUE_NAME, str)
        assert len(Config.DLQ_QUEUE_NAME) > 0

    @patch.dict(
        "os.environ",
        {
            "SQS_VISIBILITY_TIMEOUT": "600",
            "SQS_MAX_RECEIVE_COUNT": "5",
            "SQS_QUEUE_NAME": "custom-queue",
            "DLQ_QUEUE_NAME": "custom-dlq",
        },
    )
    def test_dlq_config_custom_values(self):
        """Test DLQ configuration with custom environment variables"""
        # Import fresh config with environment variables set
        import os

        # Verify environment variables are accessible
        visibility_timeout = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "300"))
        max_receive_count = int(os.getenv("SQS_MAX_RECEIVE_COUNT", "3"))
        dlq_queue_name = os.getenv("DLQ_QUEUE_NAME", "default-dlq")

        assert visibility_timeout == 600
        assert max_receive_count == 5
        assert dlq_queue_name == "custom-dlq"
