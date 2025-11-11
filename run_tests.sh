#!/bin/bash
# Script to install dependencies and run tests

echo "Installing dependencies..."
poetry install --with dev

echo "Running tests..."
poetry run pytest -v

echo "Running tests with coverage..."
poetry run pytest --cov=src --cov-report=term-missing --cov-report=html

echo "Running only unit tests..."
poetry run pytest -m unit -v

echo "Running only integration tests..."
poetry run pytest -m integration -v
