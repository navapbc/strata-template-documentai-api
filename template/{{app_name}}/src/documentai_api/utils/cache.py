"""Generic TTL cache."""

from datetime import datetime, timedelta
from typing import Any

from documentai_api.logging import get_logger

logger = get_logger(__name__)


class CacheItem:
    """Cache item with expiration."""

    def __init__(self, value: Any, ttl_minutes: int):
        self.value = value
        self.expires_at = datetime.now() + timedelta(minutes=ttl_minutes)

    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at


class Cache:
    """Generic in-memory cache with TTL."""

    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}

    def add(self, key: str, value: Any, ttl_minutes: int = 5) -> None:
        """Add item to cache with TTL."""
        self._cache[key] = CacheItem(value, ttl_minutes)
        logger.debug(f"Cache: Added '{key}' with TTL {ttl_minutes}m")

    def get(self, key: str) -> Any | None:
        """Get item from cache, None if expired or missing."""
        if key not in self._cache:
            return None

        item = self._cache[key]
        if item.is_expired():
            logger.debug(f"Cache: '{key}' expired, removing")
            del self._cache[key]
            return None

        return item.value

    def invalidate(self, key: str) -> None:
        """Remove item from cache."""
        if key in self._cache:
            del self._cache[key]
            logger.debug(f"Cache: Invalidated '{key}'")

    def clear(self) -> None:
        """Clear all cache."""
        self._cache.clear()
        logger.debug("Cache: Cleared all items")


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get global cache instance."""
    return _cache
