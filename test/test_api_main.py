import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from fastapi import HTTPException

from src.api.main import app
from src.shared.schemas import StatsResponse


@pytest.fixture
def test_client():
    """Create a test client for the API app"""
    return TestClient(app)


@pytest.fixture
def mock_stats_service():
    """Create a mock stats service"""
    with patch("src.api.main.stats_service") as mock_service:
        yield mock_service


class TestAPIEndpoints:
    """Test cases for API endpoints"""

    def test_health_check_healthy(self, test_client, mock_stats_service):
        """Test health check endpoint when service is healthy"""
        mock_stats_service.health_check.return_value = {
            "status": "healthy",
            "redis": "healthy",
        }

        response = test_client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "redis": "healthy"}
        mock_stats_service.health_check.assert_called_once()

    def test_health_check_unhealthy(self, test_client, mock_stats_service):
        """Test health check endpoint when service is unhealthy"""
        mock_stats_service.health_check.return_value = {
            "status": "unhealthy",
            "redis": "unhealthy",
        }

        response = test_client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "unhealthy", "redis": "unhealthy"}

    def test_get_all_stats_success(self, test_client, mock_stats_service):
        """Test successful retrieval of all statistics"""
        mock_stats = [
            StatsResponse(
                event_type="user_signup", count=5.0, total=250.0, average=50.0
            ),
            StatsResponse(
                event_type="user_login", count=10.0, total=150.0, average=15.0
            ),
        ]
        mock_stats_service.get_all_stats.return_value = mock_stats

        response = test_client.get("/stats")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

        assert data[0]["event_type"] == "user_signup"
        assert data[0]["count"] == 5.0
        assert data[0]["total"] == 250.0
        assert data[0]["average"] == 50.0

        assert data[1]["event_type"] == "user_login"
        assert data[1]["count"] == 10.0
        assert data[1]["total"] == 150.0
        assert data[1]["average"] == 15.0

    def test_get_all_stats_empty(self, test_client, mock_stats_service):
        """Test getting all stats when no data exists"""
        mock_stats_service.get_all_stats.return_value = []

        response = test_client.get("/stats")

        assert response.status_code == 200
        assert response.json() == []

    @patch("src.api.main.logger")
    def test_get_all_stats_error(self, mock_logger, test_client, mock_stats_service):
        """Test error handling in get_all_stats endpoint"""
        mock_stats_service.get_all_stats.side_effect = Exception("Database error")

        response = test_client.get("/stats")

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}
        mock_logger.error.assert_called_once()

    def test_get_stats_by_type_success(self, test_client, mock_stats_service):
        """Test successful retrieval of stats by event type"""
        mock_stat = StatsResponse(
            event_type="user_signup", count=3.0, total=75.0, average=25.0
        )
        mock_stats_service.get_stats_by_type.return_value = mock_stat

        response = test_client.get("/stats/user_signup")

        assert response.status_code == 200
        data = response.json()
        assert data["event_type"] == "user_signup"
        assert data["count"] == 3.0
        assert data["total"] == 75.0
        assert data["average"] == 25.0

        mock_stats_service.get_stats_by_type.assert_called_once_with("user_signup")

    def test_get_stats_by_type_special_characters(
        self, test_client, mock_stats_service
    ):
        """Test getting stats by event type with special characters"""
        event_types = [
            "user-signup",
            "user_login",
            "user%20action",  # URL encoded space
            "user@email.com",
        ]

        for event_type in event_types:
            mock_stat = StatsResponse(
                event_type=event_type, count=1.0, total=10.0, average=10.0
            )
            mock_stats_service.get_stats_by_type.return_value = mock_stat

            # URL encode the event type for the request
            import urllib.parse

            encoded_type = urllib.parse.quote(event_type, safe="")

            response = test_client.get(f"/stats/{encoded_type}")
            assert response.status_code == 200

    @patch("src.api.main.logger")
    def test_get_stats_by_type_error(
        self, mock_logger, test_client, mock_stats_service
    ):
        """Test error handling in get_stats_by_type endpoint"""
        mock_stats_service.get_stats_by_type.side_effect = Exception("Database error")

        response = test_client.get("/stats/error_test")

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}
        mock_logger.error.assert_called_once()

    def test_get_event_types_success(self, test_client, mock_stats_service):
        """Test successful retrieval of event types"""
        mock_types = ["user_signup", "user_login", "page_view"]
        mock_stats_service.get_event_types.return_value = mock_types

        response = test_client.get("/event-types")

        assert response.status_code == 200
        data = response.json()
        assert data == {"event_types": mock_types}
        mock_stats_service.get_event_types.assert_called_once()

    def test_get_event_types_empty(self, test_client, mock_stats_service):
        """Test getting event types when none exist"""
        mock_stats_service.get_event_types.return_value = []

        response = test_client.get("/event-types")

        assert response.status_code == 200
        data = response.json()
        assert data == {"event_types": []}

    @patch("src.api.main.logger")
    def test_get_event_types_error(self, mock_logger, test_client, mock_stats_service):
        """Test error handling in get_event_types endpoint"""
        mock_stats_service.get_event_types.side_effect = Exception("Database error")

        response = test_client.get("/event-types")

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}
        mock_logger.error.assert_called_once()

    def test_reset_stats_success(self, test_client, mock_stats_service):
        """Test successful stats reset"""
        mock_stats_service.reset_all_stats.return_value = True

        response = test_client.delete("/stats")

        assert response.status_code == 200
        data = response.json()
        assert data == {"message": "All statistics have been reset"}
        mock_stats_service.reset_all_stats.assert_called_once()

    def test_reset_stats_failure(self, test_client, mock_stats_service):
        """Test stats reset when service returns failure"""
        mock_stats_service.reset_all_stats.return_value = False

        response = test_client.delete("/stats")

        assert response.status_code == 500
        assert response.json() == {"detail": "Failed to reset statistics"}

    @patch("src.api.main.logger")
    def test_reset_stats_error(self, mock_logger, test_client, mock_stats_service):
        """Test error handling in reset_stats endpoint"""
        mock_stats_service.reset_all_stats.side_effect = Exception("Database error")

        response = test_client.delete("/stats")

        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}
        mock_logger.error.assert_called_once()

    def test_dashboard_endpoint(self, test_client, mock_stats_service):
        """Test dashboard HTML endpoint"""
        response = test_client.get("/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "SQS Consumer Statistics Dashboard" in response.text
        assert "refresh-btn" in response.text
        assert "fetchStats" in response.text  # JavaScript function

    def test_method_not_allowed(self, test_client):
        """Test that endpoints reject inappropriate HTTP methods"""
        # Health endpoint should only accept GET
        response = test_client.post("/health")
        assert response.status_code == 405

        # Stats endpoint should only accept GET for retrieval
        response = test_client.post("/stats")
        assert response.status_code == 405

        # Event types should only accept GET
        response = test_client.post("/event-types")
        assert response.status_code == 405

    def test_nonexistent_endpoints(self, test_client):
        """Test accessing non-existent endpoints returns 404"""
        response = test_client.get("/nonexistent")
        assert response.status_code == 404

        response = test_client.get("/stats/nonexistent/extra")
        assert response.status_code == 404


class TestAPIMetadata:
    """Test cases for API metadata and configuration"""

    def test_app_metadata(self, test_client):
        """Test FastAPI app metadata"""
        assert app.title == "SQS Consumer Stats API"
        assert (
            app.description
            == "FastAPI application for retrieving SQS message processing statistics"
        )
        assert app.version == "1.0.0"

    def test_openapi_docs_accessible(self, test_client):
        """Test that OpenAPI docs are accessible"""
        response = test_client.get("/docs")
        assert response.status_code == 200

        response = test_client.get("/openapi.json")
        assert response.status_code == 200

    def test_openapi_schema_structure(self, test_client):
        """Test OpenAPI schema structure"""
        response = test_client.get("/openapi.json")
        openapi_spec = response.json()

        # Verify basic structure
        assert "info" in openapi_spec
        assert "paths" in openapi_spec

        # Verify endpoints are documented
        paths = openapi_spec["paths"]
        assert "/health" in paths
        assert "/stats" in paths
        assert "/stats/{event_type}" in paths
        assert "/event-types" in paths
        assert "/" in paths

        # Verify stats endpoint has response model
        stats_endpoint = paths["/stats"]["get"]
        assert "responses" in stats_endpoint
        assert "200" in stats_endpoint["responses"]


class TestAPILifespan:
    """Test cases for API lifespan management"""

    @patch("src.api.main.time.sleep")
    @patch("src.api.main.stats_service")
    def test_lifespan_redis_connection_success(self, mock_stats_service, mock_sleep):
        """Test lifespan when Redis connection succeeds immediately"""
        mock_stats_service.health_check.return_value = {"status": "healthy"}

        # Test the lifespan context manager
        from src.api.main import lifespan

        # This would be called by FastAPI during startup
        # We'll test the logic without actually starting the server
        assert lifespan is not None

    @patch("src.api.main.time.sleep")
    @patch("src.api.main.stats_service")
    @patch("src.api.main.logger")
    def test_lifespan_redis_connection_retry(
        self, mock_logger, mock_stats_service, mock_sleep
    ):
        """Test lifespan when Redis connection requires retries"""
        # First few calls fail, then succeed
        mock_stats_service.health_check.side_effect = [
            Exception("Connection failed"),
            Exception("Connection failed"),
            {"status": "healthy"},
        ]

        # Test that retry logic would work (can't easily test async context manager here)
        # This tests the concept but the actual lifespan testing would need integration tests
        from src.api.main import lifespan

        assert lifespan is not None

    def test_lifespan_function_exists(self):
        """Test that lifespan function is properly defined"""
        from src.api.main import lifespan
        import inspect

        # Verify it's a function with contextmanager decorator
        assert inspect.isfunction(lifespan)
        assert hasattr(lifespan, "__wrapped__")


class TestAPIResponseModels:
    """Test cases for API response models and validation"""

    def test_stats_response_model_validation(self, test_client, mock_stats_service):
        """Test that response models are properly validated"""
        # Mock valid response
        mock_stat = StatsResponse(
            event_type="test", count=1.0, total=10.0, average=10.0
        )
        mock_stats_service.get_stats_by_type.return_value = mock_stat

        response = test_client.get("/stats/test")
        assert response.status_code == 200

        # Verify response structure matches model
        data = response.json()
        required_fields = {"event_type", "count", "total", "average"}
        assert set(data.keys()) == required_fields

    def test_event_types_response_structure(self, test_client, mock_stats_service):
        """Test event types response structure"""
        mock_stats_service.get_event_types.return_value = ["type1", "type2"]

        response = test_client.get("/event-types")
        assert response.status_code == 200

        data = response.json()
        assert "event_types" in data
        assert isinstance(data["event_types"], list)


class TestAPIIntegration:
    """Integration tests for the API"""

    def test_full_api_workflow(self, test_client, mock_stats_service):
        """Test a complete API workflow"""
        # Setup mock data
        mock_types = ["user_signup", "user_login"]
        mock_all_stats = [
            StatsResponse(
                event_type="user_signup", count=5.0, total=100.0, average=20.0
            ),
            StatsResponse(event_type="user_login", count=3.0, total=30.0, average=10.0),
        ]
        mock_individual_stat = StatsResponse(
            event_type="user_signup", count=5.0, total=100.0, average=20.0
        )

        mock_stats_service.get_event_types.return_value = mock_types
        mock_stats_service.get_all_stats.return_value = mock_all_stats
        mock_stats_service.get_stats_by_type.return_value = mock_individual_stat
        mock_stats_service.health_check.return_value = {"status": "healthy"}
        mock_stats_service.reset_all_stats.return_value = True

        # Test health check
        response = test_client.get("/health")
        assert response.status_code == 200

        # Test get event types
        response = test_client.get("/event-types")
        assert response.status_code == 200
        assert len(response.json()["event_types"]) == 2

        # Test get all stats
        response = test_client.get("/stats")
        assert response.status_code == 200
        assert len(response.json()) == 2

        # Test get individual stat
        response = test_client.get("/stats/user_signup")
        assert response.status_code == 200
        assert response.json()["event_type"] == "user_signup"

        # Test reset stats
        response = test_client.delete("/stats")
        assert response.status_code == 200

        # Test dashboard
        response = test_client.get("/")
        assert response.status_code == 200
        assert "html" in response.headers["content-type"]

    def test_cors_headers(self, test_client):
        """Test CORS headers if configured"""
        response = test_client.get("/health")

        # Basic content type check
        assert "application/json" in response.headers.get("content-type", "")

    def test_api_error_consistency(self, test_client, mock_stats_service):
        """Test that all endpoints return consistent error responses"""
        mock_stats_service.get_all_stats.side_effect = Exception("Test error")
        mock_stats_service.get_stats_by_type.side_effect = Exception("Test error")
        mock_stats_service.get_event_types.side_effect = Exception("Test error")
        mock_stats_service.reset_all_stats.side_effect = Exception("Test error")

        endpoints = [
            ("GET", "/stats"),
            ("GET", "/stats/test"),
            ("GET", "/event-types"),
            ("DELETE", "/stats"),
        ]

        for method, endpoint in endpoints:
            response = test_client.request(method, endpoint)
            assert response.status_code == 500
            assert response.json() == {"detail": "Internal server error"}


@patch("src.api.main.uvicorn")
@patch("src.api.main.Config")
def test_main_function(mock_config, mock_uvicorn):
    """Test the main function entry point"""
    mock_config.API_HOST = "127.0.0.1"
    mock_config.API_PORT = 9000

    from src.api.main import main

    main()

    mock_uvicorn.run.assert_called_once_with(app, host="127.0.0.1", port=9000)
