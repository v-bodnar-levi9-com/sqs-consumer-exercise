# NPS Kata - Event Processing System

A microservices application for processing e-commerce website events with real-time statistics.

## Quick Start

Start all services:
```bash
docker-compose up --build
```

Access the dashboard at: http://localhost:8080/docs

## Testing

Run tests:
```bash
poetry run pytest
```

## Event Schema

Events are in JSON format:

```json
{
    "type": "page_view",
    "value": 1,
    "occurred_at": "2020-10-06 10:02:05"
}
```

Expected output:

```json
{
    "type": "purchase",
    "count": 75,
    "sum": 650.0
}
```

## Configuration

The application supports the following environment variables:

### SQS Configuration
- `SQS_QUEUE_NAME`: Main queue name (default: "hands-on-interview")
- `SQS_VISIBILITY_TIMEOUT`: Message visibility timeout in seconds (default: 300)
- `SQS_MAX_RECEIVE_COUNT`: Max times a message can be received before moving to DLQ (default: 3)
- `DLQ_QUEUE_NAME`: Dead Letter Queue name (default: "{SQS_QUEUE_NAME}-dlq")
- `MAX_MESSAGES_PER_BATCH`: Max messages to receive per batch (default: 10)
- `SQS_WAIT_TIME_SECONDS`: Long polling wait time (default: 20)

### Message Processing Reliability
The application implements several production-ready features:

1. **Dead Letter Queue (DLQ)**: Failed messages are automatically moved to a DLQ after exceeding the max receive count
2. **Visibility Timeout**: Messages being processed are hidden from other consumers to prevent duplicate processing
3. **Receive Count Tracking**: Monitors how many times a message has been received to identify problematic messages
4. **Graceful Error Handling**: Messages with invalid JSON or schema are safely removed without affecting the queue

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


### What AI and for what was used

Being transparent (and even proud) about using AI-assisted coding, I will list which tools were used during the exercise.

The first step was making a production-like setup. The idea was to containerize the complete application and run everything with Docker Compose. AI here was used in agent mode with a clear task to create Dockerfiles for the producer, localstack, and consumer services.

Then we switched to use Poetry instead of pip with requirements, and AI was used to generate the pyproject.toml file.

A major milestone was the decoupling of the message processing and statistics services. The idea was that we would like to scale message processing horizontally. AI was used again here for new Docker containers, and for the API service, it created API endpoints (and even simple HTML for a dashboard, which was later removed in order not to bloat the app).

The last major use of AI was for the Redis client, as this part is pretty much standard boilerplate code to wrap the Redis library.

As for tests, they were completely generated, yet carefully reviewed.

To sum up, the AI agent (Claude 4 Sonnet) enabled me to accomplish significantly more work in a short timeframe by automating configuration, container-related, and testing tasks.