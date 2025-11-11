import logging
from typing import Dict, List
from ..shared.redis_client import redis_client
from ..shared.schemas import StatsResponse, EventStats

logger = logging.getLogger(__name__)


class StatsService:
    """Service for managing and retrieving statistics"""
    
    def __init__(self):
        self.redis = redis_client
    
    def get_all_stats(self) -> List[StatsResponse]:
        """Get statistics for all event types"""
        try:
            stats_dict = self.redis.get_all_stats()
            
            return [
                StatsResponse(
                    event_type=event_type,
                    count=stats.count,
                    total=stats.total,
                    average=stats.average
                )
                for event_type, stats in stats_dict.items()
            ]
        except Exception as e:
            logger.error(f"Error retrieving all stats: {e}")
            return []
    
    def get_stats_by_type(self, event_type: str) -> StatsResponse:
        """Get statistics for a specific event type"""
        try:
            stats = self.redis.get_event_stats(event_type)
            
            if stats is None:
                return StatsResponse(
                    event_type=event_type,
                    count=0,
                    total=0,
                    average=0
                )
            
            return StatsResponse(
                event_type=event_type,
                count=stats.count,
                total=stats.total,
                average=stats.average
            )
        except Exception as e:
            logger.error(f"Error retrieving stats for {event_type}: {e}")
            return StatsResponse(
                event_type=event_type,
                count=0,
                total=0,
                average=0
            )
    
    def get_event_types(self) -> List[str]:
        """Get all event types that have been processed"""
        try:
            return self.redis.get_all_event_types()
        except Exception as e:
            logger.error(f"Error retrieving event types: {e}")
            return []
    
    def reset_all_stats(self) -> bool:
        """Reset all statistics (useful for testing/admin)"""
        try:
            self.redis.reset_stats()
            logger.info("All statistics have been reset")
            return True
        except Exception as e:
            logger.error(f"Error resetting stats: {e}")
            return False
    
    def health_check(self) -> Dict[str, str]:
        """Check the health of the stats service"""
        try:
            redis_status = "healthy" if self.redis.ping() else "unhealthy"
            
            return {
                "status": "healthy" if redis_status == "healthy" else "unhealthy",
                "redis": redis_status
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "redis": "unhealthy",
                "error": str(e)
            }


# Global stats service instance
stats_service = StatsService()
