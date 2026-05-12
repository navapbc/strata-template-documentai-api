"""Tests for utils/auth.py."""

import hashlib
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from documentai_api.schemas.api_key import ApiKeyRecord
from documentai_api.utils import auth as auth_util
from documentai_api.utils import env
from documentai_api.utils.cache import get_cache


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the in-memory auth cache and lastUsed debounce state between tests."""
    get_cache().clear()
    auth_util._last_used_written_at.clear()
    yield
    get_cache().clear()
    auth_util._last_used_written_at.clear()


##############################################################################
# _hash_key
##############################################################################


def test_hash_key_returns_sha256():
    result = auth_util._hash_key("my-api-key")
    expected = hashlib.sha256(b"my-api-key").hexdigest()
    assert result == expected


def test_hash_key_different_inputs_produce_different_hashes():
    assert auth_util._hash_key("key-1") != auth_util._hash_key("key-2")


##############################################################################
# _get_cache_ttl
##############################################################################


def test_get_cache_ttl_default(monkeypatch):
    monkeypatch.delenv(env.API_AUTH_CACHE_TTL, raising=False)
    assert auth_util._get_cache_ttl_minutes() == 5


def test_get_cache_ttl_from_env(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_CACHE_TTL, "120")
    assert auth_util._get_cache_ttl_minutes() == 2


def test_get_cache_ttl_invalid_value(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_CACHE_TTL, "not-a-number")
    assert auth_util._get_cache_ttl_minutes() == 5


##############################################################################
# _get_from_cache / _set_cache
##############################################################################


def test_cache_miss_returns_none():
    assert get_cache().get("nonexistent-hash") is None


def test_cache_hit_returns_record():
    record = {ApiKeyRecord.KEY_HASH: "abc", ApiKeyRecord.IS_ACTIVE: True}
    get_cache().add("abc", record, ttl_minutes=5)
    assert get_cache().get("abc") == record


def test_cache_expired_returns_none():
    from datetime import datetime, timedelta

    from documentai_api.utils.cache import CacheItem

    record = {ApiKeyRecord.KEY_HASH: "abc", ApiKeyRecord.IS_ACTIVE: True}
    get_cache().add("abc", record, ttl_minutes=5)

    # manually expire the cache entry
    expired_item = CacheItem(record, ttl_minutes=1)
    expired_item.expires_at = datetime.now() - timedelta(minutes=1)
    get_cache()._cache["abc"] = expired_item

    assert get_cache().get("abc") is None


##############################################################################
# _validate_key_record
##############################################################################


def test_validate_key_record_active():
    assert auth_util._validate_key_record({ApiKeyRecord.IS_ACTIVE: True}) is True


def test_validate_key_record_inactive():
    assert auth_util._validate_key_record({ApiKeyRecord.IS_ACTIVE: False}) is False


def test_validate_key_record_missing_is_active():
    assert auth_util._validate_key_record({}) is False


def test_validate_key_record_not_expired():
    from datetime import UTC, datetime, timedelta

    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    assert (
        auth_util._validate_key_record(
            {ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.EXPIRES_AT: future}
        )
        is True
    )


def test_validate_key_record_expired():
    from datetime import UTC, datetime, timedelta

    past = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    assert (
        auth_util._validate_key_record(
            {ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.EXPIRES_AT: past}
        )
        is False
    )


def test_validate_key_record_invalid_expires_at():
    assert (
        auth_util._validate_key_record(
            {ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.EXPIRES_AT: "not-a-date"}
        )
        is False
    )


##############################################################################
# _verify_with_insecure_shared_key
##############################################################################


def test_insecure_key_missing_env_raises_500(monkeypatch):
    monkeypatch.delenv(env.API_AUTH_INSECURE_SHARED_KEY, raising=False)
    with pytest.raises(HTTPException) as exc_info:
        auth_util._verify_with_insecure_shared_key("any-key")
    assert exc_info.value.status_code == 500


def test_insecure_key_invalid_raises_401(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_INSECURE_SHARED_KEY, "correct-key")
    with pytest.raises(HTTPException) as exc_info:
        auth_util._verify_with_insecure_shared_key("wrong-key")
    assert exc_info.value.status_code == 401


def test_insecure_key_missing_header_raises_401(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_INSECURE_SHARED_KEY, "correct-key")
    with pytest.raises(HTTPException) as exc_info:
        auth_util._verify_with_insecure_shared_key(None)
    assert exc_info.value.status_code == 401


def test_insecure_key_valid_passes(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_INSECURE_SHARED_KEY, "correct-key")
    auth_util._verify_with_insecure_shared_key("correct-key")  # should not raise


##############################################################################
# _verify_with_ddb
##############################################################################


def test_ddb_verify_missing_key_raises_401():
    with pytest.raises(HTTPException) as exc_info:
        auth_util._verify_with_ddb(None)
    assert exc_info.value.status_code == 401


def test_ddb_verify_key_not_in_ddb_raises_401(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.utils.auth._lookup_key_in_ddb", return_value=None):
        with pytest.raises(HTTPException) as exc_info:
            auth_util._verify_with_ddb("docai_" + "a" * 32)
        assert exc_info.value.status_code == 401


def test_ddb_verify_inactive_key_raises_401(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch(
        "documentai_api.utils.auth._lookup_key_in_ddb",
        return_value={ApiKeyRecord.IS_ACTIVE: False, ApiKeyRecord.CLIENT_NAME: "test-client"},
    ):
        with pytest.raises(HTTPException) as exc_info:
            auth_util._verify_with_ddb("docai_" + "a" * 32)
        assert exc_info.value.status_code == 401


def test_ddb_verify_valid_key_passes(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch(
        "documentai_api.utils.auth._lookup_key_in_ddb",
        return_value={ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.CLIENT_NAME: "test-client"},
    ):
        auth_util._verify_with_ddb("docai_" + "a" * 32)  # should not raise


def test_ddb_verify_uses_cache_on_second_call(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    record = {ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.CLIENT_NAME: "test-client"}
    with patch("documentai_api.utils.auth._lookup_key_in_ddb", return_value=record) as mock_lookup:
        auth_util._verify_with_ddb("docai_" + "a" * 32)
        auth_util._verify_with_ddb("docai_" + "a" * 32)
        mock_lookup.assert_called_once()  # second call should hit cache, not DDB


##############################################################################
# _is_valid_key_format
##############################################################################


def test_valid_key_format():
    assert auth_util._is_valid_key_format("docai_" + "a" * 32) is True


def test_invalid_key_format_wrong_prefix():
    assert auth_util._is_valid_key_format("dde_prod_" + "a" * 32) is False


def test_invalid_key_format_too_short():
    assert auth_util._is_valid_key_format("docai_short") is False


def test_invalid_key_format_empty():
    assert auth_util._is_valid_key_format("") is False


def test_invalid_key_format_none():
    assert auth_util._is_valid_key_format(None) is False


def test_ddb_verify_rejects_invalid_format():
    with pytest.raises(HTTPException) as exc_info:
        auth_util._verify_with_ddb("not-a-valid-key")
    assert exc_info.value.status_code == 401


##############################################################################
# _update_last_used
##############################################################################


def test_update_last_used_debounced_skips_second_call(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.services.ddb.update_item") as mock_update:
        auth_util._update_last_used("test-hash")
        auth_util._update_last_used("test-hash")  # should be skipped
        mock_update.assert_called_once()


def test_update_last_used_writes_after_debounce_period(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.services.ddb.update_item") as mock_update:
        auth_util._update_last_used("test-hash")
        # expire the debounce window
        auth_util._last_used_written_at["test-hash"] = 0
        auth_util._update_last_used("test-hash")  # should write again
        assert mock_update.call_count == 2


def test_update_last_used_writes_to_ddb(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.services.ddb.update_item") as mock_update:
        auth_util._update_last_used("test-hash")
        mock_update.assert_called_once()
        kwargs = mock_update.call_args.kwargs
        assert ":lastUsed" in kwargs["expression_values"]
        assert kwargs["key"] == {ApiKeyRecord.KEY_HASH: "test-hash"}


def test_update_last_used_silently_ignores_errors(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.services.ddb.update_item", side_effect=Exception("DDB error")):
        auth_util._update_last_used("test-hash")  # should not raise


def test_ddb_verify_fires_last_used_update(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    record = {ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.CLIENT_NAME: "test-client"}
    with (
        patch("documentai_api.utils.auth._lookup_key_in_ddb", return_value=record),
        patch("documentai_api.utils.auth._update_last_used") as mock_last_used,
        patch("threading.Thread") as mock_thread,
    ):
        mock_thread.return_value.start = lambda: mock_last_used(
            auth_util._hash_key("docai_" + "a" * 32)
        )
        auth_util._verify_with_ddb("docai_" + "a" * 32)
        mock_thread.assert_called_once()


##############################################################################
# generate_api_key
##############################################################################


def test_generate_api_key_returns_key_and_no_existing(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with (
        patch("documentai_api.utils.auth.get_active_keys_for_client", return_value=[]),
        patch("documentai_api.services.ddb.put_item") as mock_put,
    ):
        api_key, existing = auth_util.generate_api_key("my-service", "prod")

    assert api_key.startswith("docai_")
    assert existing == []
    mock_put.assert_called_once()


def test_generate_api_key_warns_on_existing_keys(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    existing_record = {ApiKeyRecord.IS_ACTIVE: True, ApiKeyRecord.CLIENT_NAME: "my-service"}
    with (
        patch(
            "documentai_api.utils.auth.get_active_keys_for_client", return_value=[existing_record]
        ),
        patch("documentai_api.services.ddb.put_item"),
    ):
        api_key, existing = auth_util.generate_api_key("my-service", "prod")

    assert api_key.startswith("docai_")
    assert len(existing) == 1


def test_generate_api_key_stores_hash_not_plaintext(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with (
        patch("documentai_api.utils.auth.get_active_keys_for_client", return_value=[]),
        patch("documentai_api.services.ddb.put_item") as mock_put,
    ):
        api_key, _ = auth_util.generate_api_key("my-service", "prod")

    mock_put.assert_called_once()
    _, item = mock_put.call_args.args
    assert item[ApiKeyRecord.KEY_HASH] == auth_util._hash_key(api_key)
    assert api_key not in str(item)


def test_generate_api_key_with_expires_at(monkeypatch):
    from datetime import UTC, datetime, timedelta

    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    expires = datetime.now(UTC) + timedelta(days=90)
    with (
        patch("documentai_api.utils.auth.get_active_keys_for_client", return_value=[]),
        patch("documentai_api.services.ddb.put_item") as mock_put,
    ):
        auth_util.generate_api_key("my-service", "prod", expires_at=expires)

    item = mock_put.call_args[0][1]
    assert ApiKeyRecord.EXPIRES_AT in item


##############################################################################
# deactivate_api_key
##############################################################################


def test_deactivate_api_key_found(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    record = {ApiKeyRecord.KEY_HASH: "test-hash", ApiKeyRecord.IS_ACTIVE: True}
    with (
        patch("documentai_api.services.ddb.get_item", return_value=record),
        patch("documentai_api.services.ddb.update_item") as mock_update,
    ):
        result = auth_util.deactivate_api_key("test-hash")

    assert result is True
    mock_update.assert_called_once()
    kwargs = mock_update.call_args.kwargs
    assert kwargs["expression_values"][":isActive"] is False


def test_deactivate_api_key_not_found(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.services.ddb.get_item", return_value=None):
        result = auth_util.deactivate_api_key("nonexistent-hash")

    assert result is False


def test_deactivate_api_key_invalidates_cache(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    record = {ApiKeyRecord.KEY_HASH: "test-hash", ApiKeyRecord.IS_ACTIVE: True}

    get_cache().add("test-hash", record, ttl_minutes=5)
    assert get_cache().get("test-hash") is not None

    with (
        patch("documentai_api.services.ddb.get_item", return_value=record),
        patch("documentai_api.services.ddb.update_item"),
    ):
        auth_util.deactivate_api_key("test-hash")

    assert get_cache().get("test-hash") is None


def test_deactivate_api_key_with_real_ddb(api_keys_table):
    """Test deactivate_api_key updates DDB and invalidates cache."""
    api_key, _ = auth_util.generate_api_key("my-service", "prod")
    key_hash = auth_util._hash_key(api_key)

    result = auth_util.deactivate_api_key(key_hash)

    assert result is True

    item = api_keys_table.get_item(Key={ApiKeyRecord.KEY_HASH: key_hash})["Item"]
    assert item[ApiKeyRecord.IS_ACTIVE] is False


##############################################################################
# get_active_keys_for_client
##############################################################################


def test_get_active_keys_for_client_returns_matching(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    records = [
        {ApiKeyRecord.CLIENT_NAME: "my-service", ApiKeyRecord.IS_ACTIVE: True},
        {ApiKeyRecord.CLIENT_NAME: "other-service", ApiKeyRecord.IS_ACTIVE: True},
        {ApiKeyRecord.CLIENT_NAME: "my-service", ApiKeyRecord.IS_ACTIVE: False},
    ]
    with patch("documentai_api.services.ddb.scan", return_value=records):
        result = auth_util.get_active_keys_for_client("my-service")

    assert len(result) == 1
    assert result[0][ApiKeyRecord.CLIENT_NAME] == "my-service"
    assert result[0][ApiKeyRecord.IS_ACTIVE] is True


def test_get_active_keys_for_client_returns_empty_on_error(monkeypatch):
    monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
    with patch("documentai_api.services.ddb.scan", side_effect=Exception("DDB error")):
        result = auth_util.get_active_keys_for_client("my-service")

    assert result == []


##############################################################################
# verify_api_key (integration)
##############################################################################


def test_verify_api_key_uses_ddb_when_enabled(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_ENABLED, "true")
    with patch("documentai_api.utils.auth._verify_with_ddb") as mock_ddb:
        auth_util.verify_api_key("docai_" + "a" * 32)
        mock_ddb.assert_called_once_with("docai_" + "a" * 32)


def test_verify_api_key_uses_insecure_key_when_disabled(monkeypatch):
    monkeypatch.setenv(env.API_AUTH_ENABLED, "false")
    with patch("documentai_api.utils.auth._verify_with_insecure_shared_key") as mock_insecure:
        auth_util.verify_api_key("docai_" + "a" * 32)
        mock_insecure.assert_called_once_with("docai_" + "a" * 32)


def test_verify_api_key_disabled_by_default(monkeypatch):
    monkeypatch.delenv(env.API_AUTH_ENABLED, raising=False)
    with patch("documentai_api.utils.auth._verify_with_insecure_shared_key") as mock_insecure:
        auth_util.verify_api_key("docai_" + "a" * 32)
        mock_insecure.assert_called_once_with("docai_" + "a" * 32)


##############################################################################
# moto-backed tests
##############################################################################


def test_lookup_key_in_ddb_found(api_keys_table):
    """Test _lookup_key_in_ddb returns record when key exists in DDB."""
    key_hash = auth_util._hash_key("test-key")
    api_keys_table.put_item(
        Item={
            ApiKeyRecord.KEY_HASH: key_hash,
            ApiKeyRecord.CLIENT_NAME: "test-client",
            ApiKeyRecord.IS_ACTIVE: True,
        }
    )

    result = auth_util._lookup_key_in_ddb(key_hash)

    assert result is not None
    assert result[ApiKeyRecord.CLIENT_NAME] == "test-client"
    assert result[ApiKeyRecord.IS_ACTIVE] is True


def test_lookup_key_in_ddb_not_found(api_keys_table):
    """Test _lookup_key_in_ddb returns None when key does not exist."""
    result = auth_util._lookup_key_in_ddb("nonexistent-hash")
    assert result is None


def test_generate_api_key_writes_to_ddb(api_keys_table):
    """Test generate_api_key stores the hash in DDB."""
    api_key, existing = auth_util.generate_api_key("my-service", "prod")

    assert existing == []
    assert api_key.startswith("docai_")

    key_hash = auth_util._hash_key(api_key)
    result = api_keys_table.get_item(Key={ApiKeyRecord.KEY_HASH: key_hash})
    item = result.get("Item")

    assert item is not None
    assert item[ApiKeyRecord.CLIENT_NAME] == "my-service"
    assert item[ApiKeyRecord.IS_ACTIVE] is True
    assert ApiKeyRecord.CREATED_AT in item
    assert ApiKeyRecord.EXPIRES_AT not in item


def test_generate_api_key_warns_existing_via_ddb(api_keys_table):
    """Test generate_api_key detects existing active keys via real DDB scan."""
    existing_hash = auth_util._hash_key("existing-key")
    api_keys_table.put_item(
        Item={
            ApiKeyRecord.KEY_HASH: existing_hash,
            ApiKeyRecord.CLIENT_NAME: "my-service",
            ApiKeyRecord.IS_ACTIVE: True,
        }
    )

    _, existing = auth_util.generate_api_key("my-service", "prod")

    assert len(existing) == 1
    assert existing[0][ApiKeyRecord.CLIENT_NAME] == "my-service"


def test_verify_api_key_end_to_end_with_moto(api_keys_table, monkeypatch):
    """Test full verify_api_key → _verify_with_ddb → DDB flow using moto."""
    monkeypatch.setenv(env.API_AUTH_ENABLED, "true")

    # generate a real key and store it
    api_key, _ = auth_util.generate_api_key("test-client", "prod")

    # verify it passes
    auth_util.verify_api_key(api_key)  # should not raise


def test_verify_api_key_end_to_end_invalid_key(api_keys_table, monkeypatch):
    """Test full flow rejects a key that doesn't exist in DDB."""
    monkeypatch.setenv(env.API_AUTH_ENABLED, "true")

    with pytest.raises(HTTPException) as exc_info:
        auth_util.verify_api_key("docai_invalid_key")
    assert exc_info.value.status_code == 401


def test_verify_api_key_end_to_end_deactivated_key(api_keys_table, monkeypatch):
    """Test full flow rejects a deactivated key."""
    monkeypatch.setenv(env.API_AUTH_ENABLED, "true")

    api_key, _ = auth_util.generate_api_key("test-client", "prod")
    key_hash = auth_util._hash_key(api_key)

    auth_util.deactivate_api_key(key_hash)

    with pytest.raises(HTTPException) as exc_info:
        auth_util.verify_api_key(api_key)
    assert exc_info.value.status_code == 401


def test_get_active_keys_for_client_with_real_ddb(api_keys_table):
    """Test get_active_keys_for_client scans and filters correctly."""
    api_keys_table.put_item(
        Item={
            ApiKeyRecord.KEY_HASH: "hash-1",
            ApiKeyRecord.CLIENT_NAME: "my-service",
            ApiKeyRecord.IS_ACTIVE: True,
        }
    )
    api_keys_table.put_item(
        Item={
            ApiKeyRecord.KEY_HASH: "hash-2",
            ApiKeyRecord.CLIENT_NAME: "my-service",
            ApiKeyRecord.IS_ACTIVE: False,
        }
    )
    api_keys_table.put_item(
        Item={
            ApiKeyRecord.KEY_HASH: "hash-3",
            ApiKeyRecord.CLIENT_NAME: "other-service",
            ApiKeyRecord.IS_ACTIVE: True,
        }
    )

    result = auth_util.get_active_keys_for_client("my-service")

    assert len(result) == 1
    assert result[0][ApiKeyRecord.KEY_HASH] == "hash-1"
