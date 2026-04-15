import pytest


@pytest.fixture
def ddb_doc_metadata_table(ddb_doc_metadata_table_resource, set_ddb_doc_metadata_table_env_vars):
    return ddb_doc_metadata_table_resource


@pytest.fixture
def ddb_doc_metadata_table_resource(aws_credentials):
    """Create a test DynamoDB table."""
    import boto3
    from moto import mock_aws

    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="metadata",
            KeySchema=[{"AttributeName": "fileName", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "fileName", "AttributeType": "S"},
                {"AttributeName": "jobId", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "job-id-index",
                    "KeySchema": [{"AttributeName": "jobId", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table


@pytest.fixture
def set_ddb_doc_metadata_table_env_vars(ddb_doc_metadata_table_resource, monkeypatch):

    monkeypatch.setenv(
        "DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME", ddb_doc_metadata_table_resource.name
    )
    monkeypatch.setenv("DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME", "job-id-index")
    # required by get_aws_config() but not used by DDB operations
    monkeypatch.setenv("DOCUMENTAI_INPUT_LOCATION", "s3://test/input")
    monkeypatch.setenv("DOCUMENTAI_OUTPUT_LOCATION", "s3://test/output")
    monkeypatch.setenv("BDA_PROJECT_ARN", "arn:aws:test")
    monkeypatch.setenv("BDA_PROFILE_ARN", "arn:aws:test")
