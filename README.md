# NPS Kata

A prototype was developed to analyze the performance of a small e-commerce website.

The website wants to analyze different type of events that are being emitted real-time.
Given an **SQS** queue with messages representing real time events this application keeps reading those messages and prints information about the amount of particular event types and the sum of the property value.

## Setup

1. Make sure the project requirements have been installed from `requirements.txt`
2. Run the backend event server using `./.server/run.sh` (this requires an installation of docker on your machine)

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
