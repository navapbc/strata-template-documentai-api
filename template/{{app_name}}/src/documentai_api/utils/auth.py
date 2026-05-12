"""API key authentication utilities."""

import hashlib
import os
import secrets
import threading
import time
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader

from documentai_api.config.constants import API_AUTH_KEY_HEADER_NAME
from documentai_api.logging import get_logger
from documentai_api.schemas.api_key import ApiKeyRecord
from documentai_api.utils import env
from documentai_api.utils.cache import get_cache

logger = get_logger(__name__)

api_key_header = APIKeyHeader(name=API_AUTH_KEY_HEADER_NAME, auto_error=False)

# tracks when lastUsed was last written per key hash: {key_hash: monotonic_time}
_last_used_written_at: dict[str, float] = {}
_last_used_lock = threading.Lock()
_LAST_USED_DEBOUNCE_SECONDS = 60  # write at most once per minute per key


def _hash_key(api_key: str) -> str:
    """Return SHA-256 hash of the given API key."""
    return hashlib.sha256(api_key.encode()).hexdigest()


def _get_cache_ttl_minutes() -> int:
    """Get cache TTL in minutes from env, defaulting to 5.

    Note: API_AUTH_CACHE_TTL is specified in seconds (e.g. 300),
    but Cache.add() takes minutes, so we convert here.
    """
    try:
        seconds = int(os.getenv(env.API_AUTH_CACHE_TTL, "300"))
        return max(1, seconds // 60)
    except ValueError:
        return 5


def _lookup_key_in_ddb(key_hash: str) -> dict[str, Any] | None:
    """Look up an API key record from DynamoDB by its hash."""
    from documentai_api.services import ddb as ddb_service

    table_name = env.get_required_env(env.API_KEYS_TABLE_NAME)
    key = {ApiKeyRecord.KEY_HASH: key_hash}

    try:
        return ddb_service.get_item(table_name, key)
    except Exception as e:
        # If DDB is unavailable, we return None which results in a 401.
        # This is a safe default — failing open would be a security risk.
        # Callers should monitor for elevated 401 rates as a signal of DDB issues.
        logger.error(f"Failed to look up API key in DynamoDB: {e}")
        return None


def _validate_key_record(record: dict[str, Any]) -> bool:
    """Validate that the key record is active and not expired."""
    if not record.get(ApiKeyRecord.IS_ACTIVE, False):
        return False

    expires_at = record.get(ApiKeyRecord.EXPIRES_AT)
    if expires_at:
        try:
            expiry = datetime.fromisoformat(expires_at)
            if datetime.now(UTC) > expiry:
                return False
        except ValueError:
            logger.warning(f"Invalid expiresAt format: {expires_at}")
            return False

    return True


def _update_last_used(key_hash: str) -> None:
    """Best-effort async update of lastUsed timestamp. Runs in a daemon thread.

    Debounced to at most once per minute per key to avoid DDB write amplification
    under high traffic.
    """
    now = time.monotonic()

    with _last_used_lock:
        last_written = _last_used_written_at.get(key_hash, 0)
        if now - last_written < _LAST_USED_DEBOUNCE_SECONDS:
            return
        _last_used_written_at[key_hash] = now

    try:
        from documentai_api.services import ddb as ddb_service

        table_name = env.get_required_env(env.API_KEYS_TABLE_NAME)
        ddb_service.update_item(
            table_name,
            key={ApiKeyRecord.KEY_HASH: key_hash},
            update_expression=f"SET {ApiKeyRecord.LAST_USED} = :lastUsed",
            expression_values={":lastUsed": datetime.now(UTC).isoformat()},
        )
    except Exception as e:
        # reset the timestamp so the next request retries the write
        with _last_used_lock:
            _last_used_written_at.pop(key_hash, None)
        logger.warning(f"Failed to update lastUsed for key: {e}")


_API_KEY_PREFIX = "docai_"
_API_KEY_MIN_LENGTH = len(_API_KEY_PREFIX) + 32


def _is_valid_key_format(api_key: str) -> bool:
    """Return True if the key matches the expected format."""
    return (
        bool(api_key)
        and api_key.startswith(_API_KEY_PREFIX)
        and len(api_key) >= _API_KEY_MIN_LENGTH
    )


def _verify_with_ddb(api_key: str) -> None:
    """Validate API key against DynamoDB table with in-memory caching."""
    if not _is_valid_key_format(api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    key_hash = _hash_key(api_key)

    cache = get_cache()
    record = cache.get(key_hash)
    if record is None:
        record = _lookup_key_in_ddb(key_hash)
        if record:
            cache.add(key_hash, record, ttl_minutes=_get_cache_ttl_minutes())

    if not record:
        logger.warning("API key not found in DynamoDB")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    if not _validate_key_record(record):
        logger.warning(
            f"API key validation failed for client: {record.get(ApiKeyRecord.CLIENT_NAME, 'unknown')}"
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    threading.Thread(target=_update_last_used, args=(key_hash,), daemon=True).start()


def _verify_with_insecure_shared_key(api_key: str) -> None:
    """Validate API key against a single shared key (for local dev only)."""
    expected_key = os.getenv(env.API_AUTH_INSECURE_SHARED_KEY)

    if not expected_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="API key not configured"
        )

    if not api_key or not secrets.compare_digest(api_key, expected_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


def get_active_keys_for_client(client_name: str) -> list[dict[str, Any]]:
    """Return all active DynamoDB records for a given client name.

    Note: performs a table scan — acceptable for low-volume admin operations.
    """
    from documentai_api.services import ddb as ddb_service

    table_name = env.get_required_env(env.API_KEYS_TABLE_NAME)

    try:
        all_items = ddb_service.scan(table_name)
        return [
            item
            for item in all_items
            if item.get(ApiKeyRecord.CLIENT_NAME) == client_name
            and item.get(ApiKeyRecord.IS_ACTIVE, False)
        ]
    except Exception as e:
        logger.error(f"Failed to scan api-keys table: {e}")
        return []


def generate_api_key(
    client_name: str,
    environment: str,
    expires_at: datetime | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Generate a new API key, store its hash in DynamoDB, and return the plaintext key.

    The plaintext key is returned once and never stored. Only the SHA-256 hash
    is persisted in DynamoDB.

    Args:
        client_name: Name of the calling system (e.g. "my-service").
        environment: Deployment environment (e.g. "prod", "staging").
        expires_at: Optional expiry datetime. If None, the key never expires.

    Returns:
        Tuple of (plaintext API key, list of existing active records for this client).
        The caller should warn the user if existing_keys is non-empty.
    """
    from documentai_api.services import ddb as ddb_service

    existing_keys = get_active_keys_for_client(client_name)

    random_part = secrets.token_urlsafe(32)[:32]
    api_key = f"docai_{random_part}"
    key_hash = _hash_key(api_key)

    table_name = env.get_required_env(env.API_KEYS_TABLE_NAME)

    item: dict[str, Any] = {
        ApiKeyRecord.KEY_HASH: key_hash,
        ApiKeyRecord.CLIENT_NAME: client_name,
        ApiKeyRecord.IS_ACTIVE: True,
        ApiKeyRecord.CREATED_AT: datetime.now(UTC).isoformat(),
    }

    if expires_at:
        item[ApiKeyRecord.EXPIRES_AT] = expires_at.isoformat()

    ddb_service.put_item(table_name, item)
    logger.info(f"Generated API key for client: {client_name} in environment: {environment}")

    return api_key, existing_keys


def deactivate_api_key(key_hash: str) -> bool:
    """Deactivate an API key by setting isActive=false in DynamoDB.

    Args:
        key_hash: SHA-256 hash of the key to deactivate.

    Returns:
        True if the key was found and deactivated, False if not found.
    """
    from documentai_api.services import ddb as ddb_service

    table_name = env.get_required_env(env.API_KEYS_TABLE_NAME)
    key = {ApiKeyRecord.KEY_HASH: key_hash}

    existing = ddb_service.get_item(table_name, key)
    if not existing:
        return False

    ddb_service.update_item(
        table_name,
        key=key,
        update_expression=f"SET {ApiKeyRecord.IS_ACTIVE} = :isActive",
        expression_values={":isActive": False},
    )

    client_name = existing.get(ApiKeyRecord.CLIENT_NAME, "unknown")
    logger.info(f"Deactivated API key for client: {client_name}")

    # invalidate cache so deactivation takes effect immediately
    get_cache().invalidate(key_hash)

    return True


def verify_api_key(api_key: str = Depends(api_key_header)) -> None:
    """Verify the API key from the request header.

    When API_AUTH_ENABLED is true, validates against DynamoDB api-keys table
    with in-memory caching. Falls back to insecure shared key for local dev.
    """
    auth_enabled = os.getenv(env.API_AUTH_ENABLED, "false").lower() == "true"

    if auth_enabled:
        _verify_with_ddb(api_key)
    else:
        _verify_with_insecure_shared_key(api_key)
