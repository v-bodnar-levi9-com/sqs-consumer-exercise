FROM python:3.10-slim

ENV POETRY_VERSION=1.8.3 \
	POETRY_HOME=/opt/poetry \
	POETRY_NO_INTERACTION=1 \
	PIP_DISABLE_PIP_VERSION_CHECK=on

# Install system deps (if any needed later) and Poetry
RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential && \
	curl -sSL https://install.python-poetry.org | python3 - && \
	ln -s $POETRY_HOME/bin/poetry /usr/local/bin/poetry && \
	apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only dependency manifests first for caching
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --only main

# Copy source code
COPY src ./src
# Optionally include server tools (not required for consumer but harmless, aids debugging)
## (Server tooling not needed for consumer container; omitted)

# Default command can be overridden; run app
CMD ["poetry", "run", "nps-kata"]
