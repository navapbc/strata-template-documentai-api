"""Shared test fixtures."""

from unittest.mock import patch

import pytest
from moto import mock_aws


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
def mock_bda_clients():
    """Mock BDA clients (not supported by moto yet)."""
    with (
        patch("documentai_api.services.bda.AWSClientFactory.get_bda_client") as mock_bda,
        patch(
            "documentai_api.services.bda.AWSClientFactory.get_bda_runtime_client"
        ) as mock_bda_runtime,
    ):
        yield {
            "bda": mock_bda.return_value,
            "bda_runtime": mock_bda_runtime.return_value,
        }


@pytest.fixture
def mock_bda_client(mock_bda_clients):
    return mock_bda_clients["bda"]


@pytest.fixture
def mock_bda_runtime_client(mock_bda_clients):
    return mock_bda_clients["bda_runtime"]


@pytest.fixture
def mock_grayscale_dependencies():
    with (
        patch("cv2.imdecode") as mock_cv2_imdecode,
        patch("cv2.cvtColor") as mock_cv2_cvtcolor,
        patch("PIL.Image.fromarray") as mock_pil_fromarray,
    ):
        yield mock_cv2_imdecode, mock_cv2_cvtcolor, mock_pil_fromarray


@pytest.fixture
def mock_metrics_aggregator_env():
    """Mock environment and Athena dependencies for metrics aggregator tests."""
    from documentai_api.utils import env

    with (
        patch("documentai_api.tasks.metrics_aggregator.main._execute_athena_query") as mock_athena,
        patch("documentai_api.tasks.metrics_aggregator.main._get_athena_results") as mock_results,
        patch.dict(
            "os.environ",
            {
                env.DOCUMENTAI_GLUE_DATABASE_NAME: "test_db",
                env.DOCUMENTAI_METRICS_RAW_TABLE_NAME: "test_table",
                env.DOCUMENTAI_ATHENA_WORKGROUP_NAME: "test_workgroup",
                env.DOCUMENTAI_METRICS_BUCKET_NAME: "test-bucket",
            },
        ),
    ):
        mock_athena.return_value = "test-query-execution-id"
        mock_results.return_value = [
            {"process_status": "success", "created_at": "2026-02-20T10:00:00Z"}
        ]
        yield {"mock_athena": mock_athena, "mock_results": mock_results}
