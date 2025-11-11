import pytest
import json
import collections
from unittest.mock import Mock, patch, call
from pydantic import ValidationError

from src.process import process_messages


class TestProcessMessages:
    """Test cases for the process_messages function"""
    
    def setup_method(self):
        """Setup test data for each test"""
        self.test_queue_url = "http://localhost:4566/000000000000/test-queue"
        self.event_counts = collections.defaultdict(float)
        self.event_sums = collections.defaultdict(float)
    
    @patch('src.process.client')
    def test_process_messages_no_messages(self, mock_client):
        """Test processing when no messages are received"""
        # Mock SQS response with no messages
        mock_client.receive_message.return_value = {}
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        mock_client.receive_message.assert_called_once_with(
            QueueUrl=self.test_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        
        # Verify counters weren't updated
        assert len(self.event_counts) == 0
        assert len(self.event_sums) == 0
    
    @patch('src.process.client')
    def test_process_messages_valid_single_message(self, mock_client):
        """Test processing a single valid message"""
        # Create a valid message
        message_body = {"type": "user_signup", "value": 42}
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle-1"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        # Verify message was processed correctly
        assert self.event_counts["user_signup"] == 1
        assert self.event_sums["user_signup"] == 42.0
        
        # Verify message was deleted
        mock_client.delete_message.assert_called_once_with(
            QueueUrl=self.test_queue_url,
            ReceiptHandle="test-receipt-handle-1"
        )
    
    @patch('src.process.client')
    def test_process_messages_multiple_valid_messages(self, mock_client):
        """Test processing multiple valid messages"""
        messages = [
            {"Body": json.dumps({"type": "user_signup", "value": 10}), "ReceiptHandle": "handle1"},
            {"Body": json.dumps({"type": "user_login", "value": 5}), "ReceiptHandle": "handle2"},
            {"Body": json.dumps({"type": "user_signup", "value": 15}), "ReceiptHandle": "handle3"},
        ]
        
        mock_client.receive_message.return_value = {"Messages": messages}
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        # Verify counters were updated correctly
        assert self.event_counts["user_signup"] == 2
        assert self.event_sums["user_signup"] == 25.0
        assert self.event_counts["user_login"] == 1
        assert self.event_sums["user_login"] == 5.0
        
        # Verify all messages were deleted
        expected_delete_calls = [
            call(QueueUrl=self.test_queue_url, ReceiptHandle="handle1"),
            call(QueueUrl=self.test_queue_url, ReceiptHandle="handle2"),
            call(QueueUrl=self.test_queue_url, ReceiptHandle="handle3"),
        ]
        mock_client.delete_message.assert_has_calls(expected_delete_calls)
    
    @patch('src.process.client')
    @patch('src.process.logger')
    def test_process_messages_invalid_json(self, mock_logger, mock_client):
        """Test processing messages with invalid JSON"""
        messages = [
            {"Body": "invalid-json", "ReceiptHandle": "handle1"},
            {"Body": json.dumps({"type": "user_signup", "value": 10}), "ReceiptHandle": "handle2"},
        ]
        
        mock_client.receive_message.return_value = {"Messages": messages}
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        # Verify only valid message was processed
        assert self.event_counts["user_signup"] == 1
        assert self.event_sums["user_signup"] == 10.0
        
        # Verify warning was logged for invalid JSON
        mock_logger.warning.assert_called()
        warning_message = mock_logger.warning.call_args[0][0]
        assert "invalid JSON" in warning_message
        
        # Verify both messages were deleted (even the invalid one)
        assert mock_client.delete_message.call_count == 2
    
    @patch('src.process.client')
    @patch('src.process.logger')
    def test_process_messages_invalid_schema(self, mock_logger, mock_client):
        """Test processing messages with invalid schema"""
        messages = [
            {"Body": json.dumps({"type": "user_signup"}), "ReceiptHandle": "handle1"},  # missing value
            {"Body": json.dumps({"value": 10}), "ReceiptHandle": "handle2"},  # missing type
            {"Body": json.dumps({"type": "user_login", "value": 5}), "ReceiptHandle": "handle3"},  # valid
        ]
        
        mock_client.receive_message.return_value = {"Messages": messages}
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        # Verify only valid message was processed
        assert self.event_counts["user_login"] == 1
        assert self.event_sums["user_login"] == 5.0
        assert len(self.event_counts) == 1
        
        # Verify warnings were logged for invalid schema
        assert mock_logger.warning.call_count == 2
        
        # Verify all messages were deleted
        assert mock_client.delete_message.call_count == 3
    
    @patch('src.process.client')
    def test_process_messages_float_values(self, mock_client):
        """Test processing messages with float values"""
        message_body = {"type": "user_rating", "value": 4.5}
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        assert self.event_counts["user_rating"] == 1
        assert self.event_sums["user_rating"] == 4.5
    
    @patch('src.process.client')
    def test_process_messages_negative_values(self, mock_client):
        """Test processing messages with negative values"""
        message_body = {"type": "adjustment", "value": -25.5}
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        assert self.event_counts["adjustment"] == 1
        assert self.event_sums["adjustment"] == -25.5
    
    @patch('src.process.client')
    def test_process_messages_zero_value(self, mock_client):
        """Test processing messages with zero value"""
        message_body = {"type": "reset", "value": 0}
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        assert self.event_counts["reset"] == 1
        assert self.event_sums["reset"] == 0.0
    
    @patch('src.process.client')
    def test_process_messages_string_numeric_values(self, mock_client):
        """Test processing messages with string numeric values (should be coerced)"""
        message_body = {"type": "conversion_test", "value": "123.45"}
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        assert self.event_counts["conversion_test"] == 1
        assert self.event_sums["conversion_test"] == 123.45
    
    @patch('src.process.client')
    def test_process_messages_accumulation(self, mock_client):
        """Test that counters accumulate correctly over multiple calls"""
        # First batch of messages
        messages_batch1 = [
            {"Body": json.dumps({"type": "page_view", "value": 1}), "ReceiptHandle": "handle1"},
            {"Body": json.dumps({"type": "click", "value": 2.5}), "ReceiptHandle": "handle2"},
        ]
        
        mock_client.receive_message.return_value = {"Messages": messages_batch1}
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        # Verify first batch results
        assert self.event_counts["page_view"] == 1
        assert self.event_sums["page_view"] == 1.0
        assert self.event_counts["click"] == 1
        assert self.event_sums["click"] == 2.5
        
        # Second batch of messages
        messages_batch2 = [
            {"Body": json.dumps({"type": "page_view", "value": 3}), "ReceiptHandle": "handle3"},
            {"Body": json.dumps({"type": "page_view", "value": 2}), "ReceiptHandle": "handle4"},
            {"Body": json.dumps({"type": "click", "value": 1.5}), "ReceiptHandle": "handle5"},
        ]
        
        mock_client.receive_message.return_value = {"Messages": messages_batch2}
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        # Verify accumulated results
        assert self.event_counts["page_view"] == 3
        assert self.event_sums["page_view"] == 6.0
        assert self.event_counts["click"] == 2
        assert self.event_sums["click"] == 4.0
    
    @patch('src.process.client')
    def test_process_messages_empty_string_type(self, mock_client):
        """Test processing messages with empty string type (should be valid)"""
        message_body = {"type": "", "value": 42}
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        assert self.event_counts[""] == 1
        assert self.event_sums[""] == 42.0
    
    @patch('src.process.client')
    def test_process_messages_extra_fields_ignored(self, mock_client):
        """Test that extra fields in message body are ignored"""
        message_body = {
            "type": "user_signup",
            "value": 42,
            "extra_field": "should_be_ignored",
            "timestamp": "2023-01-01T00:00:00Z"
        }
        
        mock_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": json.dumps(message_body),
                    "ReceiptHandle": "test-receipt-handle"
                }
            ]
        }
        
        process_messages(self.test_queue_url, self.event_counts, self.event_sums)
        
        assert self.event_counts["user_signup"] == 1
        assert self.event_sums["user_signup"] == 42.0
        
        mock_client.delete_message.assert_called_once()


class TestProcessLogging:
    """Test cases for logging in process module"""
    
    @patch('src.process.logger')
    def test_logger_configuration(self, mock_logger):
        """Test that logger is properly configured"""
        # Import should set up logger
        from src.process import logger as process_logger
        
        # Logger should be configured with the module name
        assert process_logger.name == 'src.process'
