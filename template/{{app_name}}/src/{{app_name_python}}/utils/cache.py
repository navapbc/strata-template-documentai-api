"""Generic TTL cache."""

import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


class CacheItem:
    """Cache item with expiration."""

    def __init__(self, value: Any, ttl_minutes: int):
        self.value = value
        self.expires_at = datetime.now() + timedelta(minutes=ttl_minutes)

    def is_expired(self) -> bool:
        return datetime.now() > self.expires_at


class Cache:
    """Generic in-memory cache with TTL."""

    def __init__(self):
        self._cache = {}

    def add(self, key: str, value: Any, ttl_minutes: int = 5):
        """Add item to cache with TTL."""
        self._cache[key] = CacheItem(value, ttl_minutes)
        msg = f"Cache: Added '{key}' with TTL {ttl_minutes}m"
        print(msg)
        logger.debug(msg)

    def get(self, key: str) -> Any | None:
        """Get item from cache, None if expired or missing."""
        if key not in self._cache:
            return None

        item = self._cache[key]
        if item.is_expired():
            msg = f"Cache: '{key}' expired, removing"
            print(msg)
            logger.debug(msg)
            del self._cache[key]
            return None

        return item.value

    def invalidate(self, key: str):
        """Remove item from cache."""
        if key in self._cache:
            del self._cache[key]
            msg = f"Cache: Invalidated '{key}'"
            print(msg)
            logger.debug(msg)

    def clear(self):
        """Clear all cache."""
        self._cache.clear()
        msg = "Cache: Cleared all items"
        print(msg)
        logger.debug(msg)


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get global cache instance."""
    return _cache
