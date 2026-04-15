"""Tests for utils/aws_client_factory.py."""

from unittest.mock import MagicMock, patch

import pytest

from documentai_api.utils.aws_client_factory import AWSClientFactory


@pytest.fixture(autouse=True)
def clear_lru_cache():
    """Clear LRU cache between tests."""
    AWSClientFactory.get_s3_client.cache_clear()
    AWSClientFactory.get_dynamodb_resource.cache_clear()
    AWSClientFactory.get_bda_client.cache_clear()
    AWSClientFactory.get_bda_runtime_client.cache_clear()
    AWSClientFactory.get_ssm_client.cache_clear()
    return


@pytest.fixture
def mock_boto3_session_class():
    """Mock boto3.Session class for testing session creation.

    Use this when testing get_session() itself - it lets the real get_session()
    code execute but replaces boto3.Session with a mock.

    Yields the mock class so you can assert 'mock.assert_called_once_with(profile_name=...)'
    """
    with patch("documentai_api.utils.aws_client_factory.boto3.Session") as mock_class:
        mock_class.return_value = MagicMock()
        yield mock_class


@pytest.fixture
def mock_aws_session_instance():
    """Mock AWSClientFactory.get_session() for testing client getters.

    Use this when testing get_s3_client(), get_bda_client(), etc. - it replaces
    get_session() entirely to enable testing the client getters call on the session object.

    Yields the mock session instance so you can assert 'mock.client.assert_called_once_with("s3")'
    """
    with patch(
        "documentai_api.utils.aws_client_factory.AWSClientFactory.get_session"
    ) as mock_get_session:
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        yield mock_session


def test_get_session_singleton(clear_env_vars, mock_boto3_session_class):
    """Test that get_session() returns the same session instance (singleton pattern).

    Flow:
        1. Call get_session() twice
        2. Assert both calls return the same object
        3. Assert boto3.Session() was only called once (not twice)
    """
    session1 = AWSClientFactory.get_session()
    session2 = AWSClientFactory.get_session()

    assert session1 is session2
    mock_boto3_session_class.assert_called_once()


def test_get_region_default(clear_env_vars):
    """Test that _get_region() returns default when AWS_REGION not set."""
    region = AWSClientFactory._get_region()
    assert region == "us-east-1"


def test_get_region_from_env(monkeypatch):
    """Test that _get_region() returns value from AWS_REGION env var."""
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    region = AWSClientFactory._get_region()
    assert region == "us-west-2"


def test_get_bda_region_default(clear_env_vars):
    """Test that _get_bda_region() returns default when BDA_REGION not set."""
    region = AWSClientFactory._get_bda_region()
    assert region == "us-east-1"


def test_get_bda_region_from_env(monkeypatch):
    """Test that _get_bda_region() returns value from BDA_REGION env var."""
    monkeypatch.setenv("BDA_REGION", "eu-west-1")
    region = AWSClientFactory._get_bda_region()
    assert region == "eu-west-1"


def test_get_s3_client(mock_aws_session_instance):
    """Test that get_s3_client() calls session.client("s3").

    Flow:
        1. get_s3_client() calls get_session() - mock returns fake session
        2. Real code calls session.client("s3", region_name="us-east-1")
        3. Assert the fake session's .client() method was called correctly
    """
    client = AWSClientFactory.get_s3_client()
    mock_aws_session_instance.client.assert_called_once_with("s3", region_name="us-east-1")
    assert client is not None


def test_get_s3_client_cached(mock_aws_session_instance):
    """Test that get_s3_client() caches the client (LRU cache).

    Calling get_s3_client() twice should return the same client and only call
    session.client() once.
    """
    client1 = AWSClientFactory.get_s3_client()
    client2 = AWSClientFactory.get_s3_client()

    assert client1 is client2
    mock_aws_session_instance.client.assert_called_once()


def test_get_dynamodb_resource(mock_aws_session_instance):
    """Test that get_dynamodb_resource() calls session.resource("dynamodb")."""
    resource = AWSClientFactory.get_dynamodb_resource()
    mock_aws_session_instance.resource.assert_called_once_with("dynamodb", region_name="us-east-1")
    assert resource is not None


def test_get_bda_client(mock_aws_session_instance):
    """Test that get_bda_client() calls session.client("bedrock-data-automation")."""
    client = AWSClientFactory.get_bda_client()

    mock_aws_session_instance.client.assert_called_once_with(
        "bedrock-data-automation", region_name="us-east-1"
    )
    assert client is not None


def test_get_bda_runtime_client(mock_aws_session_instance):
    """Test that get_bda_runtime_client() calls session.client("bedrock-data-automation-runtime")."""
    client = AWSClientFactory.get_bda_runtime_client()

    mock_aws_session_instance.client.assert_called_once_with(
        "bedrock-data-automation-runtime", region_name="us-east-1"
    )
    assert client is not None


def test_get_ssm_client(mock_aws_session_instance):
    """Test that get_ssm_client() calls session.client("ssm")."""
    client = AWSClientFactory.get_ssm_client()
    mock_aws_session_instance.client.assert_called_once_with("ssm", region_name="us-east-1")
    assert client is not None


def test_get_ddb_table():
    """Test that get_ddb_table() calls get_dynamodb_resource().Table(table_name)."""
    with patch.object(AWSClientFactory, "get_dynamodb_resource") as mock_get_ddb_resource:
        mock_resource_obj = MagicMock()
        mock_get_ddb_resource.return_value = mock_resource_obj
        mock_table = MagicMock()
        mock_resource_obj.Table.return_value = mock_table

        table = AWSClientFactory.get_ddb_table("test-table")

        mock_resource_obj.Table.assert_called_once_with("test-table")
        assert table is mock_table
