import pytest
import os
from unittest.mock import patch

from src.shared.config import Config, REDIS_COUNT_KEY, REDIS_SUM_KEY, REDIS_EVENTS_SET


class TestConfig:
    """Test cases for the Config class"""

    def test_default_values(self):
        """Test that default configuration values are correct"""
        # Test AWS configuration defaults
        assert Config.AWS_ENDPOINT_URL == "http://localstack:4566"
        assert Config.AWS_ACCESS_KEY_ID == "test"
        assert Config.AWS_SECRET_ACCESS_KEY == "test"
        assert Config.AWS_DEFAULT_REGION == "us-east-1"

        # Test SQS configuration defaults
        assert Config.SQS_QUEUE_NAME == "hands-on-interview"

        # Test Redis configuration defaults
        assert Config.REDIS_HOST == "redis"
        assert Config.REDIS_PORT == 6379
        assert Config.REDIS_DB == 0

        # Test processing configuration defaults
        assert Config.MAX_MESSAGES_PER_BATCH == 10
        assert Config.SQS_WAIT_TIME_SECONDS == 20
        assert Config.PROCESSOR_SLEEP_INTERVAL == 1

        # Test API configuration defaults
        assert Config.API_HOST == "0.0.0.0"
        assert Config.API_PORT == 8000

        # Test logging configuration default
        assert Config.LOG_LEVEL == "INFO"

    @patch.dict(
        os.environ,
        {
            "AWS_ENDPOINT_URL": "http://custom-aws:4567",
            "AWS_ACCESS_KEY_ID": "custom-access-key",
            "AWS_SECRET_ACCESS_KEY": "custom-secret-key",
            "AWS_DEFAULT_REGION": "eu-west-1",
        },
    )
    def test_integer_type_conversion(self):
        """Test that string environment variables are properly converted to integers"""
        with patch.dict(
            os.environ,
            {
                "REDIS_PORT": "6379",
                "REDIS_DB": "0",
                "MAX_MESSAGES_PER_BATCH": "10",
                "SQS_WAIT_TIME_SECONDS": "20",
                "PROCESSOR_SLEEP_INTERVAL": "1",
                "API_PORT": "8000",
            },
        ):
            from src.shared.config import Config

            # Verify all are integers, not strings
            assert isinstance(Config.REDIS_PORT, int)
            assert isinstance(Config.REDIS_DB, int)
            assert isinstance(Config.MAX_MESSAGES_PER_BATCH, int)
            assert isinstance(Config.SQS_WAIT_TIME_SECONDS, int)
            assert isinstance(Config.PROCESSOR_SLEEP_INTERVAL, int)
            assert isinstance(Config.API_PORT, int)

    @patch.dict(os.environ, {"REDIS_PORT": "invalid_number"}, clear=False)
    def test_invalid_integer_environment_variable(self):
        """Test behavior when invalid integer is provided in environment variable"""
        with pytest.raises(ValueError):
            # This should raise ValueError when trying to convert 'invalid_number' to int
            from importlib import reload
            from src.shared import config

            reload(config)

    def test_missing_environment_variables(self):
        """Test that missing environment variables use defaults"""
        # Clear all relevant environment variables
        env_vars_to_clear = [
            "AWS_ENDPOINT_URL",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
            "SQS_QUEUE_NAME",
            "REDIS_HOST",
            "REDIS_PORT",
            "REDIS_DB",
            "MAX_MESSAGES_PER_BATCH",
            "SQS_WAIT_TIME_SECONDS",
            "PROCESSOR_SLEEP_INTERVAL",
            "API_HOST",
            "API_PORT",
            "LOG_LEVEL",
        ]

        # Create a clean environment
        clean_env = {k: v for k, v in os.environ.items() if k not in env_vars_to_clear}

        with patch.dict(os.environ, clean_env, clear=True):
            from importlib import reload
            from src.shared import config

            reload(config)

            # All values should be defaults
            assert config.Config.AWS_ENDPOINT_URL == "http://localstack:4566"
            assert config.Config.SQS_QUEUE_NAME == "hands-on-interview"
            assert config.Config.REDIS_HOST == "redis"
            assert config.Config.LOG_LEVEL == "INFO"


class TestRedisConstants:
    """Test cases for Redis key constants"""

    def test_redis_count_key_format(self):
        """Test that Redis count key format is correct"""
        expected = "stats:count:{event_type}"
        assert REDIS_COUNT_KEY == expected

        # Test formatting works correctly
        formatted_key = REDIS_COUNT_KEY.format(event_type="user_signup")
        assert formatted_key == "stats:count:user_signup"

    def test_redis_sum_key_format(self):
        """Test that Redis sum key format is correct"""
        expected = "stats:sum:{event_type}"
        assert REDIS_SUM_KEY == expected

        # Test formatting works correctly
        formatted_key = REDIS_SUM_KEY.format(event_type="user_login")
        assert formatted_key == "stats:sum:user_login"

    def test_redis_events_set_key(self):
        """Test that Redis events set key is correct"""
        expected = "stats:event_types"
        assert REDIS_EVENTS_SET == expected

    def test_key_uniqueness(self):
        """Test that all Redis keys are unique"""
        keys = [REDIS_COUNT_KEY, REDIS_SUM_KEY, REDIS_EVENTS_SET]
        assert len(keys) == len(set(keys))

    def test_key_formatting_with_special_characters(self):
        """Test key formatting with special characters in event type"""
        event_types = [
            "user-signup",
            "user_login",
            "user.logout",
            "user:action",
            "user@email",
            "user signup",
        ]

        for event_type in event_types:
            count_key = REDIS_COUNT_KEY.format(event_type=event_type)
            sum_key = REDIS_SUM_KEY.format(event_type=event_type)

            assert count_key.startswith("stats:count:")
            assert sum_key.startswith("stats:sum:")
            assert event_type in count_key
            assert event_type in sum_key

    def test_key_formatting_with_empty_event_type(self):
        """Test key formatting with empty event type"""
        count_key = REDIS_COUNT_KEY.format(event_type="")
        sum_key = REDIS_SUM_KEY.format(event_type="")

        assert count_key == "stats:count:"
        assert sum_key == "stats:sum:"
