"""Test fixtures relating to AWS."""

import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def fix_aws_client_factory():
    from documentai_api.utils.aws_client_factory import AWSClientFactory

    AWSClientFactory._session = None


@pytest.fixture
def aws_credentials(monkeypatch):
    """Mock AWS credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_client(aws_credentials):
    """Create a test S3 client."""
    import boto3

    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_bucket(aws_credentials):
    """Create a test S3 bucket resource."""
    import boto3

    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        bucket = s3.Bucket("test-bucket")
        bucket.create()
        yield bucket


@pytest.fixture
def ddb_table(aws_credentials):
    """Create a test DynamoDB table."""
    import boto3

    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="test-table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "userId", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "test-index",
                    "KeySchema": [{"AttributeName": "userId", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.test_index_name = "test-index"
        yield table


@pytest.fixture
def mock_bda_clients(mocker):
    """Mock BDA clients (not supported by moto yet)."""
    mock_bda = mocker.patch("documentai_api.services.bda.AWSClientFactory.get_bda_client")
    mock_bda_runtime = mocker.patch(
        "documentai_api.services.bda.AWSClientFactory.get_bda_runtime_client"
    )
    return {
        "bda": mock_bda.return_value,
        "bda_runtime": mock_bda_runtime.return_value,
    }


@pytest.fixture
def mock_bda_client(mock_bda_clients):
    return mock_bda_clients["bda"]


@pytest.fixture
def mock_bda_runtime_client(mock_bda_clients):
    return mock_bda_clients["bda_runtime"]
