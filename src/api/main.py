#!/usr/bin/env python
import logging
import time
from typing import List, Dict
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

from ..shared.config import Config
from ..shared.schemas import StatsResponse
from .stats import stats_service

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Stats API service")

    # Wait for Redis to be available
    max_retries = 30
    for attempt in range(max_retries):
        try:
            health = stats_service.health_check()
            if health["status"] == "healthy":
                logger.info("Successfully connected to Redis")
                break
        except Exception as e:
            logger.warning(
                f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}"
            )
            time.sleep(1)
    else:
        logger.error("Failed to connect to Redis after maximum retries")
        raise RuntimeError("Could not connect to Redis")

    yield

    logger.info("Shutting down Stats API service")


app = FastAPI(
    title="SQS Consumer Stats API",
    description="FastAPI application for retrieving SQS message processing statistics",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return stats_service.health_check()


@app.get("/stats", response_model=List[StatsResponse])
async def get_all_stats():
    """Get statistics for all event types"""
    try:
        stats = stats_service.get_all_stats()
        return stats
    except Exception as e:
        logger.error(f"Error retrieving all stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/stats/{event_type}", response_model=StatsResponse)
async def get_stats_by_type(event_type: str):
    """Get statistics for a specific event type"""
    try:
        stats = stats_service.get_stats_by_type(event_type)
        return stats
    except Exception as e:
        logger.error(f"Error retrieving stats for {event_type}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/")
async def index():
    return {"message": "Welcome to the SQS Consumer Stats API"}


def main():
    """Entry point for the API service"""
    import uvicorn

    logger.info(f"Starting Stats API on {Config.API_HOST}:{Config.API_PORT}")
    uvicorn.run(app, host=Config.API_HOST, port=Config.API_PORT)


if __name__ == "__main__":
    main()
