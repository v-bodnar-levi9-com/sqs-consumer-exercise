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

Beeing transparent (and even proud) about using AI assisted coding, I will list which tools was used during the exercise.

First step was making production-esque setup. Idea was to conteinerize complete application and run everything with docker compose.
AI here was used in agentic mode with clear task to create docker files for producer, localsatck  and consumer services.

Than we switched to use poetry instead of pip with requirements, and AI was used to generate pyproject toml file

Major milestone was decoupling of message processing and statistics services. Idea was that we would like to scale horisontally message processing.
AI was used again here for new docker containers, and for API service it created api endpoints (end even simple html for dashboard which was later removed in order not to bloat the app).

Last major use of AI was for redis client, as this part is prety much standart boilerplate code to wrap redis library.

As for tests - they were completely generated, yet carefully revied.

To sum up - AI agent (claude 4 sonet) enabled me to accomplish significantly more work in a short timeframe by automating configuration, container-related, and testing tasks.
