import logging
from typing import Dict, List
from ..shared.redis_client import redis_client
from ..shared.schemas import StatsResponse

logger = logging.getLogger(__name__)


class StatsService:
    """Service for managing and retrieving statistics"""

    def __init__(self):
        self.redis = redis_client

    def get_all_stats(self) -> List[StatsResponse]:
        """Get statistics for all event types"""
        stats_dict = self.redis.get_all_stats()

        return [
            StatsResponse(
                event_type=event_type,
                count=stats.count,
                total=stats.total,
                average=stats.average,
            )
            for event_type, stats in stats_dict.items()
        ]

    def get_stats_by_type(self, event_type: str) -> StatsResponse:
        """Get statistics for a specific event type"""
        stats = self.redis.get_event_stats(event_type)

        if stats is None:
            return StatsResponse(event_type=event_type, count=0, total=0, average=0)

        return StatsResponse(
            event_type=event_type,
            count=stats.count,
            total=stats.total,
            average=stats.average,
        )

    def health_check(self) -> Dict[str, str]:
        """Check the health of the stats service"""
        try:
            redis_status = "healthy" if self.redis.ping() else "unhealthy"

            return {
                "status": "healthy" if redis_status == "healthy" else "unhealthy",
                "redis": redis_status,
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"status": "unhealthy", "redis": "unhealthy", "error": str(e)}


stats_service = StatsService()
