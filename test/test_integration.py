import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient


@pytest.mark.integration
class TestApplicationIntegration:
    """Integration tests for the complete FastAPI application"""

    @patch("src.main.message_processor")
    def test_app_startup_and_health_check(self, mock_message_processor, test_client):
        """Test that app starts up correctly and health check works"""
        # Mock the background task
        mock_task = AsyncMock()
        with patch("asyncio.create_task", return_value=mock_task):
            response = test_client.get("/health")

            assert response.status_code == 200
            assert response.json() == {"status": "healthy"}

    @patch("src.main.SQSConfig")
    def test_sqs_config_integration(self, mock_sqs_config_class, test_client):
        """Test SQS config integration with FastAPI dependency injection"""
        mock_config = Mock()
        mock_sqs_config_class.return_value = mock_config

        # The get_sqs_config dependency should return the same instance
        from src.main import get_sqs_config

        config1 = get_sqs_config()
        config2 = get_sqs_config()

        # Should be the same instance (singleton pattern)
        assert config1 is config2

    def test_fastapi_openapi_integration(self, test_client):
        """Test that OpenAPI documentation is properly integrated"""
        # Test OpenAPI JSON endpoint
        response = test_client.get("/openapi.json")
        assert response.status_code == 200

        openapi_spec = response.json()

        # Verify basic structure
        assert "info" in openapi_spec
        assert "paths" in openapi_spec

        # Verify app metadata
        assert openapi_spec["info"]["title"] == "SQS Consumer API"
        assert (
            openapi_spec["info"]["description"]
            == "FastAPI application for consuming SQS messages"
        )
        assert openapi_spec["info"]["version"] == "0.1.0"

        # Verify health endpoint is documented
        assert "/health" in openapi_spec["paths"]
        health_endpoint = openapi_spec["paths"]["/health"]
        assert "get" in health_endpoint

    def test_docs_endpoint_integration(self, test_client):
        """Test that Swagger UI docs endpoint works"""
        response = test_client.get("/docs")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_redoc_endpoint_integration(self, test_client):
        """Test that ReDoc docs endpoint works"""
        response = test_client.get("/redoc")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_cors_and_middleware_integration(self, test_client):
        """Test CORS and middleware integration (if any)"""
        response = test_client.get("/health")

        # Check that basic headers are present
        assert response.status_code == 200

        # Verify response format
        assert response.headers["content-type"] == "application/json"

    def test_background_task_function_exists(self):
        """Test that background message processor function exists"""
        from src.main import message_processor
        import inspect

        # Verify the function exists and is async
        assert inspect.isfunction(message_processor)
        assert inspect.iscoroutinefunction(message_processor)

    def test_error_handling_integration(self, test_client):
        """Test error handling across the application"""
        # Test 404 for non-existent endpoints
        response = test_client.get("/non-existent-endpoint")
        assert response.status_code == 404

        # Test method not allowed
        response = test_client.post("/health")
        assert response.status_code == 405

        # Test unsupported media type for endpoints that don't accept bodies
        response = test_client.patch("/health", json={"test": "data"})
        assert response.status_code == 405


@pytest.mark.integration
class TestApplicationLifecycle:
    """Test the complete application lifecycle"""

    def test_app_has_lifespan(self):
        """Test that the app is properly configured with lifespan"""
        from src.main import app

        # Verify the app has lifespan configured
        assert hasattr(app, "router")
        assert hasattr(app.router, "lifespan_context")

        # Just test the basic app structure without starting lifespan
        # (which would try to connect to SQS)
        assert app.title == "SQS Consumer API"

    def test_dependency_injection_lifecycle(self, test_client):
        """Test that dependency injection works throughout the app lifecycle"""
        from src.main import get_sqs_config

        # Get config multiple times to ensure consistency
        configs = [get_sqs_config() for _ in range(5)]

        # All should be the same instance
        for config in configs[1:]:
            assert config is configs[0]

        # Should be SQSConfig instance
        from src.main import SQSConfig

        assert isinstance(configs[0], SQSConfig)
