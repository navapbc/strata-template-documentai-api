from freezegun import freeze_time

from documentai_api.utils.cache import Cache, CacheItem, get_cache


def test_cache_item_not_expired():
    item = CacheItem("test_value", ttl_minutes=5)
    assert item.is_expired() is False
    assert item.value == "test_value"


def test_cache_add_and_get():
    cache = Cache()
    cache.add("key1", "value1", ttl_minutes=5)
    assert cache.get("key1") == "value1"


def test_cache_get_missing_key():
    cache = Cache()
    assert cache.get("nonexistent") is None


def test_cache_get_expired_item():
    cache = Cache()

    with freeze_time("2026-01-01 12:00:00"):
        # add item with 5 minute time-to-live
        cache.add("key1", "value1", ttl_minutes=5)

        # try to get at 6 minutes later
        with freeze_time("2026-01-01 12:06:00"):
            assert cache.get("key1") is None

        # verify item was removed from cache
        assert "key1" not in cache._cache


def test_cache_invalidate_existing_key():
    cache = Cache()
    cache.add("key1", "value1")
    cache.invalidate("key1")
    assert cache.get("key1") is None


def test_cache_invalidate_nonexistent_key():
    cache = Cache()
    cache.invalidate("nonexistent")  # should not raise error
    assert cache.get("nonexistent") is None


def test_cache_clear():
    cache = Cache()
    cache.add("key1", "value1")
    cache.add("key2", "value2")
    cache.clear()
    assert cache.get("key1") is None
    assert cache.get("key2") is None


def test_get_cache_returns_singleton():
    cache1 = get_cache()
    cache2 = get_cache()
    assert cache1 is cache2
