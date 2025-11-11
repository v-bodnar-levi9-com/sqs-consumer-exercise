# NPS Kata - Decoupled Architecture

A scalable prototype for analyzing real-time e-commerce website events using a decoupled microservices architecture.

## Architecture Overview

The application has been decoupled into separate, scalable services:

### Services
- **Message Processor Service**: Scalable SQS consumer that processes messages and stores stats in Redis
- **Stats API Service**: FastAPI service that provides HTTP endpoints for retrieving statistics  
- **Redis**: Shared data store for real-time statistics aggregation
- **LocalStack**: SQS service emulation for local development

### Benefits
- ✅ **Horizontally Scalable**: Multiple processor instances can run in parallel
- ✅ **Resilient**: Statistics persist across container restarts
- ✅ **Decoupled**: Services can be deployed, scaled, and updated independently
- ✅ **Atomic Operations**: Redis ensures data consistency across multiple processors

## Quick Start

### Start the complete system:
```bash
docker-compose up
```

### Scale processor service to 5 instances:
```bash
docker-compose -f docker-compose.yaml -f docker-compose.scale.yaml up
```

### Access the dashboard:
Open http://localhost:8000 in your browser for real-time statistics

### API Endpoints:
- `GET /` - Interactive dashboard
- `GET /stats` - All event statistics
- `GET /stats/{event_type}` - Specific event statistics
- `GET /health` - Service health check
- `DELETE /stats` - Reset all statistics

## Architecture Components

### Message Processor (`src/processor/`)
- Consumes SQS messages in batches
- Validates message schemas
- Atomically updates Redis counters
- Supports graceful shutdown
- **Horizontally scalable**

### Stats API (`src/api/`)
- FastAPI service for statistics retrieval
- Real-time dashboard with auto-refresh
- RESTful API for programmatic access
- Health monitoring

### Shared Components (`src/shared/`)
- **schemas.py**: Pydantic models for validation
- **config.py**: Environment-based configuration
- **redis_client.py**: Redis operations and connection management

## Container Usage

### Build individual services:

Build processor container:
```bash
docker build -t nps-processor:latest -f Dockerfile.processor .
```

Build API container:
```bash
docker build -t nps-api:latest -f Dockerfile.api .

## Testing

This project includes a comprehensive test suite that covers all FastAPI-related functionality.

### Quick Start

Run all tests:
```bash
poetry run pytest
```

### Test Categories

- **Unit Tests**: Test individual components (schemas, endpoints, utilities)
- **Integration Tests**: Test component interactions and complete workflows
- **FastAPI Tests**: HTTP endpoints, OpenAPI documentation, middleware
- **Schema Tests**: Pydantic model validation and serialization

### Running Specific Test Types

Run only unit tests:
```bash
poetry run pytest -m unit
```

Run only integration tests:
```bash
poetry run pytest -m integration
```

Run tests for specific modules:
```bash
poetry run pytest test/test_schemas.py      # Schema validation tests
poetry run pytest test/test_fastapi_endpoints.py  # API endpoint tests
```

### Using the Test Script

A convenience script is provided for common testing scenarios:
```bash
./run_tests.sh
```

This script will:
1. Install dependencies
2. Run all tests with verbose output
3. Run unit and integration tests separately

## Event Schema

* The events are in JSON format and they look like this:

```json
{
    "type": "page_view",
    "value": 1,
    "occurred_at": "2020-10-06 10:02:05"
}
```

* An expected output of this app is

```json
{
    "type": "purchase",
    "count": 75,
    "sum": 650.0,
}
```

## Assignment

The prototype received positive evaluations, and we now want to make it ready to run in a production environment.

Your assignment is to improve the application for readability, maintainability and robustness.
The idea is not to re-write the code from scratch, but rather to practice taking small steps, running tests often, and incrementally improving the design.

Some ideas to get you started:

* Use an established python project manager like `poetry` or `uv`.
* Provide tests so you can verify your progress.
* Add a basic diagram as documentation.

This assignment focuses only on the Python code and git versioning.
You do not need to define infrastructure as code or other resources.
You still want to be able to run the application locally so include the containerization in your rework.

## A note on AI

Using an AI assistant like Copilot, etc. is allowed, but these restrictions apply:

* You're transparent about which parts you used it for. For example: generating a unit test for a function, asking questions about best practices, etc.
* Don't generate the whole assignment.
