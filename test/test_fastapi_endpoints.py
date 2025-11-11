import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from src.main import app


class TestFastAPIEndpoints:
    """Test cases for FastAPI endpoints"""
    
    def setup_method(self):
        """Setup test client for each test"""
        self.client = TestClient(app)
    
    def test_health_check_endpoint(self):
        """Test the health check endpoint"""
        response = self.client.get("/health")
        
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}
    
    def test_health_check_endpoint_method_not_allowed(self):
        """Test that health endpoint only accepts GET requests"""
        response = self.client.post("/health")
        assert response.status_code == 405
        
        response = self.client.put("/health")
        assert response.status_code == 405
        
        response = self.client.delete("/health")
        assert response.status_code == 405
    
    def test_nonexistent_endpoint(self):
        """Test accessing a non-existent endpoint returns 404"""
        response = self.client.get("/nonexistent")
        assert response.status_code == 404
    
    def test_app_metadata(self):
        """Test FastAPI app metadata"""
        assert app.title == "SQS Consumer API"
        assert app.description == "FastAPI application for consuming SQS messages"
        assert app.version == "0.1.0"
    
    def test_openapi_docs_accessible(self):
        """Test that OpenAPI docs are accessible"""
        response = self.client.get("/docs")
        assert response.status_code == 200
        
        response = self.client.get("/openapi.json")
        assert response.status_code == 200
        
        openapi_data = response.json()
        assert openapi_data["info"]["title"] == "SQS Consumer API"
        assert openapi_data["info"]["version"] == "0.1.0"
    
    def test_health_endpoint_in_openapi_spec(self):
        """Test that health endpoint is properly documented in OpenAPI spec"""
        response = self.client.get("/openapi.json")
        openapi_data = response.json()
        
        assert "/health" in openapi_data["paths"]
        health_path = openapi_data["paths"]["/health"]
        
        assert "get" in health_path
        assert health_path["get"]["summary"] == "Health Check"
        
        # Check response schema
        responses = health_path["get"]["responses"]
        assert "200" in responses
