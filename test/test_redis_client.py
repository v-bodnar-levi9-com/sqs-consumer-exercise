import pytest
import redis
from unittest.mock import Mock, patch, MagicMock

from src.shared.redis_client import RedisClient, redis_client
from src.shared.schemas import EventStats
from src.shared.config import REDIS_COUNT_KEY, REDIS_SUM_KEY, REDIS_EVENTS_SET


class TestRedisClient:
    """Test cases for the RedisClient class"""

    def setup_method(self):
        """Setup a fresh RedisClient for each test"""
        self.redis_client = RedisClient()

    @patch("src.shared.redis_client.redis.Redis")
    def test_redis_client_initialization(self, mock_redis_class):
        """Test RedisClient initialization with correct parameters"""
        mock_redis_instance = Mock()
        mock_redis_class.return_value = mock_redis_instance

        client = RedisClient()

        # Verify Redis was initialized with correct parameters
        mock_redis_class.assert_called_once_with(
            host="redis",  # from Config.REDIS_HOST
            port=6379,  # from Config.REDIS_PORT
            db=0,  # from Config.REDIS_DB
            decode_responses=True,
        )

        assert client.redis == mock_redis_instance

    def test_ping_success(self):
        """Test successful Redis ping"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.ping.return_value = True

        result = self.redis_client.ping()

        assert result is True
        self.redis_client.redis.ping.assert_called_once()

    def test_ping_connection_error(self):
        """Test Redis ping when connection fails"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.ping.side_effect = redis.ConnectionError(
            "Connection failed"
        )

        result = self.redis_client.ping()

        assert result is False
        self.redis_client.redis.ping.assert_called_once()

    def test_increment_event(self):
        """Test incrementing event count and sum"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        self.redis_client.increment_event("user_signup", 42.5)

        # Verify pipeline operations
        self.redis_client.redis.pipeline.assert_called_once()
        mock_pipeline.incrbyfloat.assert_any_call(
            REDIS_COUNT_KEY.format(event_type="user_signup"), 1
        )
        mock_pipeline.incrbyfloat.assert_any_call(
            REDIS_SUM_KEY.format(event_type="user_signup"), 42.5
        )
        mock_pipeline.sadd.assert_called_once_with(REDIS_EVENTS_SET, "user_signup")
        mock_pipeline.execute.assert_called_once()

    def test_increment_event_negative_value(self):
        """Test incrementing event with negative value"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        self.redis_client.increment_event("adjustment", -25.0)

        mock_pipeline.incrbyfloat.assert_any_call(
            REDIS_SUM_KEY.format(event_type="adjustment"), -25.0
        )

    def test_increment_event_zero_value(self):
        """Test incrementing event with zero value"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        self.redis_client.increment_event("reset", 0)

        mock_pipeline.incrbyfloat.assert_any_call(
            REDIS_SUM_KEY.format(event_type="reset"), 0
        )

    def test_get_event_stats_existing_event(self):
        """Test getting statistics for an existing event"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()
        mock_pipeline.execute.return_value = ["5.0", "125.5"]
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        result = self.redis_client.get_event_stats("user_signup")

        assert isinstance(result, EventStats)
        assert result.count == 5.0
        assert result.total == 125.5
        assert result.average == 25.1

        # Verify pipeline operations
        mock_pipeline.get.assert_any_call(
            REDIS_COUNT_KEY.format(event_type="user_signup")
        )
        mock_pipeline.get.assert_any_call(
            REDIS_SUM_KEY.format(event_type="user_signup")
        )
        mock_pipeline.execute.assert_called_once()

    def test_get_event_stats_nonexistent_event(self):
        """Test getting statistics for a non-existent event"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()
        mock_pipeline.execute.return_value = [None, None]
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        result = self.redis_client.get_event_stats("nonexistent")

        assert result is None

    def test_get_event_stats_partial_data(self):
        """Test getting statistics when only partial data exists"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()

        # Test case where count exists but sum doesn't
        mock_pipeline.execute.return_value = ["5.0", None]
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        result = self.redis_client.get_event_stats("partial")
        assert result is None

        # Test case where sum exists but count doesn't
        mock_pipeline.execute.return_value = [None, "125.5"]
        result = self.redis_client.get_event_stats("partial")
        assert result is None

    def test_get_all_event_types_empty(self):
        """Test getting all event types when none exist"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.smembers.return_value = set()

        result = self.redis_client.get_all_event_types()

        assert result == []
        self.redis_client.redis.smembers.assert_called_once_with(REDIS_EVENTS_SET)

    def test_get_all_event_types_with_data(self):
        """Test getting all event types when data exists"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.smembers.return_value = {
            "user_signup",
            "user_login",
            "page_view",
        }

        result = self.redis_client.get_all_event_types()

        assert len(result) == 3
        assert set(result) == {"user_signup", "user_login", "page_view"}

    def test_get_all_stats_empty(self):
        """Test getting all statistics when no data exists"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.smembers.return_value = set()

        result = self.redis_client.get_all_stats()

        assert result == {}

    def test_get_all_stats_with_data(self):
        """Test getting all statistics when data exists"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()

        # Mock event types
        self.redis_client.redis.smembers.return_value = {"user_signup", "user_login"}

        # Mock pipeline results: count1, sum1, count2, sum2
        mock_pipeline.execute.return_value = ["3.0", "150.0", "5.0", "75.0"]
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        result = self.redis_client.get_all_stats()

        assert len(result) == 2
        assert "user_signup" in result
        assert "user_login" in result

        # Check user_signup stats
        signup_stats = result["user_signup"]
        assert signup_stats.count == 3.0
        assert signup_stats.total == 150.0
        assert signup_stats.average == 50.0

        # Check user_login stats
        login_stats = result["user_login"]
        assert login_stats.count == 5.0
        assert login_stats.total == 75.0
        assert login_stats.average == 15.0

    def test_get_all_stats_with_missing_data(self):
        """Test getting all statistics when some data is missing"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()

        # Mock event types
        self.redis_client.redis.smembers.return_value = {"user_signup", "invalid_event"}

        # Mock pipeline results: valid count/sum, then None/None for invalid event
        mock_pipeline.execute.return_value = ["3.0", "150.0", None, None]
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        result = self.redis_client.get_all_stats()

        # Should only contain the valid event
        assert len(result) == 1
        assert "user_signup" in result
        assert "invalid_event" not in result

    def test_reset_stats_no_data(self):
        """Test resetting statistics when no data exists"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.smembers.return_value = set()

        self.redis_client.reset_stats()

        # Should not call delete when no event types exist
        self.redis_client.redis.delete.assert_not_called()

    def test_reset_stats_with_data(self):
        """Test resetting statistics when data exists"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.smembers.return_value = {"user_signup", "user_login"}

        self.redis_client.reset_stats()

        # Verify all keys are deleted
        expected_keys = [
            REDIS_COUNT_KEY.format(event_type="user_signup"),
            REDIS_SUM_KEY.format(event_type="user_signup"),
            REDIS_COUNT_KEY.format(event_type="user_login"),
            REDIS_SUM_KEY.format(event_type="user_login"),
            REDIS_EVENTS_SET,
        ]

        self.redis_client.redis.delete.assert_called_once()
        call_args = self.redis_client.redis.delete.call_args[0]
        assert len(call_args) == len(expected_keys)
        assert set(call_args) == set(expected_keys)

    @patch("src.shared.redis_client.logger")
    def test_increment_event_logging(self, mock_logger):
        """Test that increment_event logs debug message"""
        self.redis_client.redis = Mock()
        mock_pipeline = Mock()
        self.redis_client.redis.pipeline.return_value = mock_pipeline

        self.redis_client.increment_event("test_event", 42.0)

        mock_logger.debug.assert_called_once_with(
            "Incremented event test_event by value 42.0"
        )

    @patch("src.shared.redis_client.logger")
    def test_reset_stats_logging(self, mock_logger):
        """Test that reset_stats logs info message"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.smembers.return_value = {"test_event"}

        self.redis_client.reset_stats()

        mock_logger.info.assert_called_once_with("Reset all event statistics")

    def test_redis_connection_error_handling(self):
        """Test handling of Redis connection errors"""
        self.redis_client.redis = Mock()
        self.redis_client.redis.pipeline.side_effect = redis.ConnectionError(
            "Connection failed"
        )

        with pytest.raises(redis.ConnectionError):
            self.redis_client.increment_event("test_event", 1.0)


class TestRedisClientSingleton:
    """Test cases for the global redis_client instance"""

    def test_redis_client_singleton_exists(self):
        """Test that the global redis_client instance exists"""
        assert redis_client is not None
        assert isinstance(redis_client, RedisClient)

    def test_redis_client_singleton_consistency(self):
        """Test that multiple imports return the same instance"""
        from src.shared.redis_client import redis_client as client1
        from src.shared.redis_client import redis_client as client2

        assert client1 is client2
        assert client1 is redis_client


class TestEventStatsIntegration:
    """Test integration between RedisClient and EventStats"""

    def test_event_stats_average_calculation(self):
        """Test that EventStats correctly calculates averages"""
        stats = EventStats(count=4.0, total=100.0)
        assert stats.average == 25.0

        # Test zero count
        stats_zero = EventStats(count=0.0, total=50.0)
        assert stats_zero.average == 0.0

        # Test negative values
        stats_negative = EventStats(count=2.0, total=-10.0)
        assert stats_negative.average == -5.0

    def test_float_conversion_in_get_event_stats(self):
        """Test that string Redis values are correctly converted to floats"""
        client = RedisClient()
        client.redis = Mock()
        mock_pipeline = Mock()

        # Test with string numbers that should be converted to floats
        mock_pipeline.execute.return_value = ["5", "125.5"]
        client.redis.pipeline.return_value = mock_pipeline

        result = client.get_event_stats("test")

        assert result.count == 5.0
        assert result.total == 125.5
        assert isinstance(result.count, float)
        assert isinstance(result.total, float)
