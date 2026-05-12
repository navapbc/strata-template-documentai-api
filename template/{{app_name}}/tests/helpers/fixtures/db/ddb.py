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
    from documentai_api.utils import env

    monkeypatch.setenv(
        env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME, ddb_doc_metadata_table_resource.name
    )
    monkeypatch.setenv(env.DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME, "job-id-index")


@pytest.fixture
def api_keys_table(aws_credentials, monkeypatch):
    """Create a test api-keys DynamoDB table and set env var."""
    import boto3
    from moto import mock_aws

    from documentai_api.utils import env

    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="api-keys-test",
            KeySchema=[{"AttributeName": "keyHash", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "keyHash", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        monkeypatch.setenv(env.API_KEYS_TABLE_NAME, "api-keys-test")
        yield table
