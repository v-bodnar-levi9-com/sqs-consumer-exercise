import pytest
from unittest.mock import Mock, patch

from src.api.stats import StatsService, stats_service
from src.shared.schemas import StatsResponse, EventStats


class TestStatsService:
    """Test cases for the StatsService class"""

    def setup_method(self):
        """Setup a fresh StatsService for each test"""
        self.stats_service = StatsService()
        self.mock_redis = Mock()
        self.stats_service.redis = self.mock_redis

    def test_stats_service_initialization(self):
        """Test StatsService initialization"""
        service = StatsService()

        # Should have redis client from shared module
        assert hasattr(service, "redis")

    def test_get_all_stats_success(self):
        """Test successful retrieval of all statistics"""
        # Mock Redis response
        mock_stats = {
            "user_signup": EventStats(count=5.0, total=250.0),
            "user_login": EventStats(count=10.0, total=150.0),
        }
        self.mock_redis.get_all_stats.return_value = mock_stats

        result = self.stats_service.get_all_stats()

        assert len(result) == 2

        # Check first stat
        signup_stat = next(stat for stat in result if stat.event_type == "user_signup")
        assert signup_stat.count == 5.0
        assert signup_stat.total == 250.0
        assert signup_stat.average == 50.0

        # Check second stat
        login_stat = next(stat for stat in result if stat.event_type == "user_login")
        assert login_stat.count == 10.0
        assert login_stat.total == 150.0
        assert login_stat.average == 15.0

        self.mock_redis.get_all_stats.assert_called_once()

    def test_get_all_stats_empty(self):
        """Test getting all stats when no data exists"""
        self.mock_redis.get_all_stats.return_value = {}

        result = self.stats_service.get_all_stats()

        assert result == []

    def test_get_stats_by_type_existing_event(self):
        """Test getting statistics for an existing event type"""
        event_stats = EventStats(count=3.0, total=75.0)
        self.mock_redis.get_event_stats.return_value = event_stats

        result = self.stats_service.get_stats_by_type("user_signup")

        assert isinstance(result, StatsResponse)
        assert result.event_type == "user_signup"
        assert result.count == 3.0
        assert result.total == 75.0
        assert result.average == 25.0

        self.mock_redis.get_event_stats.assert_called_once_with("user_signup")

    def test_get_stats_by_type_nonexistent_event(self):
        """Test getting statistics for a non-existent event type"""
        self.mock_redis.get_event_stats.return_value = None

        result = self.stats_service.get_stats_by_type("nonexistent")

        assert isinstance(result, StatsResponse)
        assert result.event_type == "nonexistent"
        assert result.count == 0
        assert result.total == 0
        assert result.average == 0

    def test_health_check_healthy(self):
        """Test health check when Redis is healthy"""
        self.mock_redis.ping.return_value = True

        result = self.stats_service.health_check()

        expected = {"status": "healthy", "redis": "healthy"}
        assert result == expected
        self.mock_redis.ping.assert_called_once()

    def test_health_check_redis_unhealthy(self):
        """Test health check when Redis is unhealthy"""
        self.mock_redis.ping.return_value = False

        result = self.stats_service.health_check()

        expected = {"status": "unhealthy", "redis": "unhealthy"}
        assert result == expected

    @patch("src.api.stats.logger")
    def test_health_check_exception(self, mock_logger):
        """Test health check when an exception occurs"""
        self.mock_redis.ping.side_effect = Exception("Connection failed")

        result = self.stats_service.health_check()

        assert result["status"] == "unhealthy"
        assert result["redis"] == "unhealthy"
        assert "error" in result
        assert "Connection failed" in result["error"]

        mock_logger.error.assert_called_once()
        error_message = mock_logger.error.call_args[0][0]
        assert "Health check failed" in error_message


class TestStatsServiceIntegration:
    """Integration tests for StatsService with different data scenarios"""

    def setup_method(self):
        """Setup StatsService for integration tests"""
        self.stats_service = StatsService()
        self.mock_redis = Mock()
        self.stats_service.redis = self.mock_redis

    def test_complex_stats_retrieval(self):
        """Test retrieval of complex statistics with various data types"""
        mock_stats = {
            "user_signup": EventStats(count=100.0, total=2500.0),
            "user_login": EventStats(count=50.0, total=150.0),
            "page_view": EventStats(count=1000.0, total=1000.0),
            "error": EventStats(count=5.0, total=-25.0),  # negative total
            "zero_count": EventStats(count=0.0, total=0.0),
        }
        self.mock_redis.get_all_stats.return_value = mock_stats

        result = self.stats_service.get_all_stats()

        assert len(result) == 5

        # Test various calculations
        signup_stat = next(stat for stat in result if stat.event_type == "user_signup")
        assert signup_stat.average == 25.0

        error_stat = next(stat for stat in result if stat.event_type == "error")
        assert error_stat.average == -5.0

        zero_stat = next(stat for stat in result if stat.event_type == "zero_count")
        assert zero_stat.average == 0.0

    def test_service_with_large_datasets(self):
        """Test service behavior with large datasets"""
        # Simulate large number of event types
        mock_stats = {}
        expected_results = []

        for i in range(100):
            event_type = f"event_type_{i}"
            count = float(i + 1)
            total = float((i + 1) * 10)

            mock_stats[event_type] = EventStats(count=count, total=total)
            expected_results.append(
                {
                    "event_type": event_type,
                    "count": count,
                    "total": total,
                    "average": 10.0,
                }
            )

        self.mock_redis.get_all_stats.return_value = mock_stats

        result = self.stats_service.get_all_stats()

        assert len(result) == 100

        # Verify a few random entries
        for i in [0, 25, 50, 99]:
            expected = expected_results[i]
            actual = next(
                stat for stat in result if stat.event_type == expected["event_type"]
            )
            assert actual.count == expected["count"]
            assert actual.total == expected["total"]
            assert actual.average == expected["average"]


class TestStatsServiceSingleton:
    """Test cases for the global stats_service instance"""

    def test_stats_service_singleton_exists(self):
        """Test that the global stats_service instance exists"""
        assert stats_service is not None
        assert isinstance(stats_service, StatsService)

    def test_stats_service_singleton_consistency(self):
        """Test that multiple imports return the same instance"""
        from src.api.stats import stats_service as service1
        from src.api.stats import stats_service as service2

        assert service1 is service2
        assert service1 is stats_service

    def test_singleton_has_redis_client(self):
        """Test that singleton instance has Redis client"""
        assert hasattr(stats_service, "redis")
        # The redis client should be the shared instance
        from src.shared.redis_client import redis_client

        assert stats_service.redis is redis_client


class TestStatsServiceEdgeCases:
    """Test edge cases and boundary conditions"""

    def setup_method(self):
        """Setup StatsService for edge case tests"""
        self.stats_service = StatsService()
        self.mock_redis = Mock()
        self.stats_service.redis = self.mock_redis

    def test_empty_event_type_handling(self):
        """Test handling of empty event type"""
        event_stats = EventStats(count=1.0, total=5.0)
        self.mock_redis.get_event_stats.return_value = event_stats

        result = self.stats_service.get_stats_by_type("")

        assert result.event_type == ""
        assert result.count == 1.0
        assert result.total == 5.0

    def test_special_character_event_types(self):
        """Test handling of event types with special characters"""
        special_types = [
            "user-signup",
            "user_login",
            "user.logout",
            "user:action",
            "user@email.com",
            "user signup with spaces",
        ]

        for event_type in special_types:
            event_stats = EventStats(count=1.0, total=10.0)
            self.mock_redis.get_event_stats.return_value = event_stats

            result = self.stats_service.get_stats_by_type(event_type)

            assert result.event_type == event_type
            assert result.count == 1.0

    def test_very_large_numbers(self):
        """Test handling of very large numbers"""
        large_stats = EventStats(count=1e12, total=1e15)
        self.mock_redis.get_event_stats.return_value = large_stats

        result = self.stats_service.get_stats_by_type("large_numbers")

        assert result.count == 1e12
        assert result.total == 1e15
        assert result.average == 1000.0  # 1e15 / 1e12

    def test_very_small_numbers(self):
        """Test handling of very small numbers"""
        small_stats = EventStats(count=1e-6, total=1e-3)
        self.mock_redis.get_event_stats.return_value = small_stats

        result = self.stats_service.get_stats_by_type("small_numbers")

        assert result.count == 1e-6
        assert result.total == 1e-3
        assert abs(result.average - 1000.0) < 1e-10  # 1e-3 / 1e-6 = 1000
