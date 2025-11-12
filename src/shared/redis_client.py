import redis
import logging
from typing import Dict, List, Optional
from .config import Config, REDIS_COUNT_KEY, REDIS_SUM_KEY, REDIS_EVENTS_SET
from .schemas import EventStats

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis client for managing event statistics"""

    def __init__(self):
        self.redis = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB,
            decode_responses=True,
        )

    def ping(self) -> bool:
        """Check Redis connection"""
        try:
            self.redis.ping()
            return True
        except redis.ConnectionError:
            return False

    def increment_event(self, event_type: str, value: float) -> None:
        """
        Atomically increment event count and sum for a specific event type
        """
        pipe = self.redis.pipeline()

        # Use Redis pipeline for atomic operations
        pipe.incrbyfloat(REDIS_COUNT_KEY.format(event_type=event_type), 1)
        pipe.incrbyfloat(REDIS_SUM_KEY.format(event_type=event_type), value)
        pipe.sadd(REDIS_EVENTS_SET, event_type)

        pipe.execute()

        logger.debug(f"Incremented event {event_type} by value {value}")

    def get_event_stats(self, event_type: str) -> Optional[EventStats]:
        """Get statistics for a specific event type"""
        pipe = self.redis.pipeline()
        pipe.get(REDIS_COUNT_KEY.format(event_type=event_type))
        pipe.get(REDIS_SUM_KEY.format(event_type=event_type))

        count_str, sum_str = pipe.execute()

        if count_str is None or sum_str is None:
            return None

        return EventStats(count=float(count_str), total=float(sum_str))

    def get_all_event_types(self) -> List[str]:
        """Get all event types that have been processed"""
        return list(self.redis.smembers(REDIS_EVENTS_SET))

    def get_all_stats(self) -> Dict[str, EventStats]:
        """Get statistics for all event types"""
        event_types = self.get_all_event_types()

        if not event_types:
            return {}

        pipe = self.redis.pipeline()

        # Batch get all counts and sums
        for event_type in event_types:
            pipe.get(REDIS_COUNT_KEY.format(event_type=event_type))
            pipe.get(REDIS_SUM_KEY.format(event_type=event_type))

        results = pipe.execute()

        stats = {}
        for i, event_type in enumerate(event_types):
            count_str = results[i * 2]
            sum_str = results[i * 2 + 1]

            if count_str is not None and sum_str is not None:
                stats[event_type] = EventStats(
                    count=float(count_str), total=float(sum_str)
                )

        return stats

    def reset_stats(self) -> None:
        """Reset all statistics (useful for testing)"""
        event_types = self.get_all_event_types()

        if event_types:
            keys_to_delete = []
            for event_type in event_types:
                keys_to_delete.extend(
                    [
                        REDIS_COUNT_KEY.format(event_type=event_type),
                        REDIS_SUM_KEY.format(event_type=event_type),
                    ]
                )
            keys_to_delete.append(REDIS_EVENTS_SET)

            self.redis.delete(*keys_to_delete)

        logger.info("Reset all event statistics")


redis_client = RedisClient()
