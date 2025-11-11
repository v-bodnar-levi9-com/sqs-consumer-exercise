# NPS Kata

A prototype was developed to analyze the performance of a small e-commerce website.

The website wants to analyze different type of events that are being emitted real-time.
Given an **SQS** queue with messages representing real time events this application keeps reading those messages and prints information about the amount of particular event types and the sum of the property value.


## Container Usage

Build runtime container:
```bash
docker build -t nps-kata:latest -f Dockerfile .
```
Run application container (example, overriding queue name if needed):
```bash
docker run --rm -e SQS_QUEUE_NAME=hands-on-interview nps-kata:latest
```

Build event producer container:
```bash
docker build -t nps-kata-producer:latest -f Dockerfile.producer .
```
Run producer:
```bash
docker run --rm nps-kata-producer:latest
```

Both images install dependencies via Poetry directly; no `requirements.txt` is used during the build anymore.

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
