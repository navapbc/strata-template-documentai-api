"""Tests for services/bda.py."""

from unittest.mock import patch

import pytest
from moto import mock_aws

from documentai_api.services import bda as bda_service


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


def test_get_data_automation_project(mock_bda_client):
    """Get BDA project details."""
    project_arn = "arn:aws:bedrock:us-east-1:123:project/test"

    mock_bda_client.get_data_automation_project.return_value = {"projectArn": project_arn}

    result = bda_service.get_data_automation_project(project_arn)
    assert result["projectArn"] == project_arn

    mock_bda_client.get_data_automation_project.assert_called_once_with(projectArn=project_arn)


def test_get_blueprint(mock_bda_client):
    """Get blueprint schema details."""
    blueprint_arn = "arn:aws:bedrock:us-east-1:123:blueprint/test"

    mock_bda_client.get_blueprint.return_value = {"blueprintArn": blueprint_arn}

    result = bda_service.get_blueprint(blueprint_arn)
    assert result["blueprintArn"] == blueprint_arn

    mock_bda_client.get_blueprint.assert_called_once_with(blueprintArn=blueprint_arn)


def test_invoke_data_automation_async(mock_bda_runtime_client):
    """Invoke BDA job asynchronously."""
    input_config = {"s3Uri": "s3://bucket/input.pdf"}
    output_config = {"s3Uri": "s3://bucket/output/"}
    project_arn = "arn:aws:bedrock:us-east-1:123:project/test"
    invocation_arn = "arn:aws:bedrock:us-east-1:123:invocation/test"

    mock_bda_runtime_client.invoke_data_automation_async.return_value = {
        "invocationArn": invocation_arn
    }

    result = bda_service.invoke_data_automation_async(project_arn, input_config, output_config)

    assert result["invocationArn"] == invocation_arn
    mock_bda_runtime_client.invoke_data_automation_async.assert_called_once_with(
        projectArn=project_arn, inputConfiguration=input_config, outputConfiguration=output_config
    )


def test_get_data_automation_job(mock_bda_runtime_client):
    """Get BDA job status."""
    job_arn = "arn:aws:bedrock:us-east-1:123:job/test"
    mock_bda_runtime_client.get_data_automation_job.return_value = {
        "jobArn": job_arn,
        "status": "Completed",
    }

    result = bda_service.get_data_automation_job(job_arn)

    assert result["status"] == "Completed"
    mock_bda_runtime_client.get_data_automation_job.assert_called_once_with(jobArn=job_arn)


@mock_aws
def test_get_bda_result_json_success(aws_credentials):
    """Read BDA result JSON from S3."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="my-bucket")
    s3.put_object(Bucket="my-bucket", Key="path/to/result.json", Body=b'{"result": "success"}')

    result = bda_service.get_bda_result_json("s3://my-bucket/path/to/result.json")

    assert result == {"result": "success"}


def test_get_bda_result_json_empty_uri():
    """Return None for empty URI."""
    result = bda_service.get_bda_result_json("")
    assert result is None


@mock_aws
def test_get_bda_result_json_exception(aws_credentials):
    """Return None when S3 read fails."""
    result = bda_service.get_bda_result_json("s3://nonexistent-bucket/key")
    assert result is None


def test_get_bda_job_response_success(mock_bda_runtime_client):
    """Get BDA job status successfully."""
    mock_bda_runtime_client.get_data_automation_status.return_value = {"status": "InProgress"}

    result = bda_service.get_bda_job_response("arn:aws:bedrock:us-east-1:123:invocation/test")

    assert result["status"] == "InProgress"


def test_get_bda_job_response_exception(mock_bda_runtime_client):
    """Return None when get status fails."""
    mock_bda_runtime_client.get_data_automation_status.side_effect = Exception("API error")

    result = bda_service.get_bda_job_response("arn:aws:bedrock:us-east-1:123:invocation/test")

    assert result is None


@mock_aws
def test_extract_bda_output_s3_uri_custom_path(aws_credentials):
    """Extract custom output path from job metadata."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(
        Bucket="bucket",
        Key="metadata.json",
        Body=b"""{"output_metadata": [{"segment_metadata": [{"custom_output_path": "s3://bucket/custom/output.json"}]}]}""",
    )

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result == "s3://bucket/custom/output.json"


@mock_aws
def test_extract_bda_output_s3_uri_standard_path(aws_credentials):
    """Extract standard output path from job metadata."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(
        Bucket="bucket",
        Key="metadata.json",
        Body=b"""{"output_metadata": [{"segment_metadata": [{"standard_output_path": "s3://bucket/standard/output.json"}]}]}""",
    )

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result == "s3://bucket/standard/output.json"


@mock_aws
def test_extract_bda_output_s3_uri_no_path(aws_credentials):
    """Return None when no output path found."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(Bucket="bucket", Key="metadata.json", Body=b'{"output_metadata": []}')

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result is None


@mock_aws
def test_extract_bda_output_s3_uri_malformed(aws_credentials):
    """Return None when metadata is malformed."""
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(Bucket="bucket", Key="metadata.json", Body=b'{"output_metadata": "not a list"}')

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result is None
