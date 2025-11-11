#!/usr/bin/env python
import logging
import time
from typing import List, Dict
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from ..shared.config import Config
from ..shared.schemas import StatsResponse
from .stats import stats_service

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
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
            logger.warning(f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
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
    lifespan=lifespan
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
        logger.error(f"Error retrieving stats: {e}")
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


@app.get("/event-types")
async def get_event_types():
    """Get all event types that have been processed"""
    try:
        event_types = stats_service.get_event_types()
        return {"event_types": event_types}
    except Exception as e:
        logger.error(f"Error retrieving event types: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/stats")
async def reset_stats():
    """Reset all statistics (admin endpoint)"""
    try:
        success = stats_service.reset_all_stats()
        if success:
            return {"message": "All statistics have been reset"}
        else:
            raise HTTPException(status_code=500, detail="Failed to reset statistics")
    except Exception as e:
        logger.error(f"Error resetting stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Simple HTML dashboard to view stats"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>SQS Consumer Stats Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .stats-container { margin-bottom: 20px; }
            .event-card { border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; }
            .metric { display: inline-block; margin-right: 20px; }
            .metric label { font-weight: bold; }
            .refresh-btn { background-color: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
            .refresh-btn:hover { background-color: #0056b3; }
        </style>
    </head>
    <body>
        <h1>SQS Consumer Statistics Dashboard</h1>
        <button class="refresh-btn" onclick="refreshStats()">Refresh</button>
        <div id="stats-container" class="stats-container">
            Loading statistics...
        </div>

        <script>
            async function fetchStats() {
                try {
                    const response = await fetch('/stats');
                    const stats = await response.json();
                    displayStats(stats);
                } catch (error) {
                    document.getElementById('stats-container').innerHTML = 
                        '<p style="color: red;">Error loading statistics: ' + error.message + '</p>';
                }
            }

            function displayStats(stats) {
                const container = document.getElementById('stats-container');
                
                if (stats.length === 0) {
                    container.innerHTML = '<p>No statistics available yet.</p>';
                    return;
                }

                let html = '<h2>Event Statistics</h2>';
                stats.forEach(stat => {
                    html += `
                        <div class="event-card">
                            <h3>Event Type: ${stat.event_type}</h3>
                            <div class="metric">
                                <label>Count:</label> ${stat.count}
                            </div>
                            <div class="metric">
                                <label>Total:</label> ${stat.total.toFixed(2)}
                            </div>
                            <div class="metric">
                                <label>Average:</label> ${stat.average.toFixed(2)}
                            </div>
                        </div>
                    `;
                });
                
                container.innerHTML = html;
            }

            function refreshStats() {
                fetchStats();
            }

            // Initial load
            fetchStats();
            
            // Auto-refresh every 5 seconds
            setInterval(fetchStats, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


def main():
    """Entry point for the API service"""
    import uvicorn
    
    logger.info(f"Starting Stats API on {Config.API_HOST}:{Config.API_PORT}")
    uvicorn.run(app, host=Config.API_HOST, port=Config.API_PORT)


if __name__ == "__main__":
    main()
