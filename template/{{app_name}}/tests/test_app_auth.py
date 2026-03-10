"""Tests for API authentication."""

import os
from unittest.mock import patch

from documentai_api.app import get_api_key
from documentai_api.utils import env


def test_get_api_key_returns_none_when_not_set():
    """Test returns None when API_AUTH_TOKEN not set."""
    with patch.dict(os.environ, {}, clear=True):
        assert get_api_key() is None


def test_get_api_key_returns_direct_value():
    """Test returns direct token value for local dev."""
    with patch.dict(os.environ, {env.API_AUTH_TOKEN: "my-secret-token"}):
        assert get_api_key() == "my-secret-token"


def test_get_api_key_fetches_from_ssm_with_arn():
    """Test fetches from SSM when ARN is provided."""
    arn = "arn:aws:ssm:us-east-1:123456789012:parameter/app/api-token"

    with (
        patch.dict(os.environ, {env.API_AUTH_TOKEN: arn}),
        patch("documentai_api.app.get_cache") as mock_cache,
        patch("documentai_api.app.ssm_service.get_parameter") as mock_ssm,
    ):
        mock_cache_instance = mock_cache.return_value
        mock_cache_instance.get.return_value = None
        mock_ssm.return_value = "secret-from-ssm"

        result = get_api_key()

        assert result == "secret-from-ssm"
        mock_ssm.assert_called_once_with("/app/api-token")
        mock_cache_instance.add.assert_called_once_with(
            "api_auth_token", "secret-from-ssm", ttl_minutes=60
        )


def test_get_api_key_returns_cached_value():
    """Test returns cached value without calling SSM."""
    arn = "arn:aws:ssm:us-east-1:123456789012:parameter/app/api-token"

    with (
        patch.dict(os.environ, {env.API_AUTH_TOKEN: arn}),
        patch("documentai_api.app.get_cache") as mock_cache,
        patch("documentai_api.app.ssm_service.get_parameter") as mock_ssm,
    ):
        mock_cache_instance = mock_cache.return_value
        mock_cache_instance.get.return_value = "cached-token"

        result = get_api_key()

        assert result == "cached-token"
        mock_ssm.assert_not_called()
