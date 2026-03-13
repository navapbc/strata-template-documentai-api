"""Tests for API authentication."""

from unittest.mock import patch


def test_verify_api_key_missing_env_var(api_client):
    """Test returns 500 when API_AUTH_INSECURE_SHARED_KEY not set."""
    with patch("documentai_api.app.os.getenv", return_value=None):
        response = api_client.get("/v1/schemas")
        assert response.status_code == 500
        assert "API key not configured" in response.json()["detail"]


def test_verify_api_key_invalid_key(api_client):
    """Test returns 401 when API key is invalid."""
    with patch("documentai_api.app.os.getenv", return_value="correct-key"):
        response = api_client.get("/v1/schemas", headers={"X-API-Key": "wrong-key"})
        assert response.status_code == 401
        assert "Invalid API key" in response.json()["detail"]


def test_verify_api_key_missing_header(api_client):
    """Test returns 401 when API key header is missing."""
    with patch("documentai_api.app.os.getenv", return_value="correct-key"):
        response = api_client.get("/v1/schemas")
        assert response.status_code == 401
        assert "Invalid API key" in response.json()["detail"]


def test_verify_api_key_valid(api_client):
    """Test allows request with valid API key."""
    with (
        patch("documentai_api.app.os.getenv", return_value="correct-key"),
        patch("documentai_api.app.get_all_schemas", return_value={"test": {}}),
    ):
        response = api_client.get("/v1/schemas", headers={"X-API-Key": "correct-key"})
        assert response.status_code == 200
