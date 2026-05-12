"""Tests for API authentication."""

from unittest.mock import patch

##############################################################################
# insecure shared key mode (API_AUTH_ENABLED=false, default)
##############################################################################


def test_verify_api_key_missing_env_var(api_client):
    """Test returns 500 when API_AUTH_INSECURE_SHARED_KEY not set."""
    response = api_client.get("/v1/schemas")
    assert response.status_code == 500
    assert "API key not configured" in response.json()["detail"]


def test_verify_api_key_invalid_key(api_client, api_skeleton_key):
    """Test returns 401 when API key is invalid."""
    response = api_client.get("/v1/schemas", headers={"API-Key": api_skeleton_key + "extra"})
    assert response.status_code == 401
    assert "Invalid API key" in response.json()["detail"]


def test_verify_api_key_missing_header(api_client, api_skeleton_key):
    """Test returns 401 when API key header is missing."""
    response = api_client.get("/v1/schemas")
    assert response.status_code == 401
    assert "Invalid API key" in response.json()["detail"]


def test_verify_api_key_valid(api_client, api_skeleton_key, mocker):
    """Test allows request with valid API key."""
    mocker.patch("documentai_api.app.get_all_schemas", return_value={"test": {}})

    response = api_client.get("/v1/schemas", headers={"API-Key": api_skeleton_key})
    assert response.status_code == 200


##############################################################################
# DynamoDB multi-key mode (API_AUTH_ENABLED=true)
##############################################################################


def test_ddb_auth_valid_key(api_client, monkeypatch, mocker):
    """Test allows request when API key is valid in DDB."""
    monkeypatch.setenv("API_AUTH_ENABLED", "true")
    mocker.patch("documentai_api.app.get_all_schemas", return_value={"test": {}})

    with patch("documentai_api.utils.auth._verify_with_ddb"):
        response = api_client.get("/v1/schemas", headers={"API-Key": "docai_somekey"})

    assert response.status_code == 200


def test_ddb_auth_invalid_key(api_client, monkeypatch):
    """Test returns 401 when API key is not in DDB."""
    from fastapi import HTTPException

    monkeypatch.setenv("API_AUTH_ENABLED", "true")

    with patch(
        "documentai_api.utils.auth._verify_with_ddb",
        side_effect=HTTPException(status_code=401, detail="Invalid API key"),
    ):
        response = api_client.get("/v1/schemas", headers={"API-Key": "docai_badkey"})

    assert response.status_code == 401
    assert "Invalid API key" in response.json()["detail"]


def test_ddb_auth_missing_header(api_client, monkeypatch):
    """Test returns 401 when API key header is missing in DDB mode."""
    from fastapi import HTTPException

    monkeypatch.setenv("API_AUTH_ENABLED", "true")

    with patch(
        "documentai_api.utils.auth._verify_with_ddb",
        side_effect=HTTPException(status_code=401, detail="Invalid API key"),
    ):
        response = api_client.get("/v1/schemas")

    assert response.status_code == 401
