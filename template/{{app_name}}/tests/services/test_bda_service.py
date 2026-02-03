"""Tests for services/bda.py."""

from unittest.mock import MagicMock, patch

import pytest

from documentai_api.services import bda as bda_service


@pytest.fixture(autouse=True)
def mock_aws_clients():
    """Mock all AWS clients used by BDA service."""
    with (
        patch("documentai_api.services.bda.AWSClientFactory.get_bda_client") as mock_bda,
        patch(
            "documentai_api.services.bda.AWSClientFactory.get_bda_runtime_client"
        ) as mock_bda_runtime,
        patch("documentai_api.services.bda.AWSClientFactory.get_s3_client") as mock_s3,
    ):
        yield {
            "bda": mock_bda.return_value,
            "bda_runtime": mock_bda_runtime.return_value,
            "s3": mock_s3.return_value,
        }


@pytest.fixture
def mock_bda_client(mock_aws_clients):
    return mock_aws_clients["bda"]


@pytest.fixture
def mock_bda_runtime_client(mock_aws_clients):
    return mock_aws_clients["bda_runtime"]


@pytest.fixture
def mock_s3_client(mock_aws_clients):
    return mock_aws_clients["s3"]


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


def test_get_bda_result_json_success(mock_s3_client):
    """Read BDA result JSON from S3."""
    mock_body = MagicMock()
    mock_body.read.return_value = b'{"result": "success"}'
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = bda_service.get_bda_result_json("s3://my-bucket/path/to/result.json")

    assert result == {"result": "success"}
    mock_s3_client.get_object.assert_called_once_with(Bucket="my-bucket", Key="path/to/result.json")


def test_get_bda_result_json_empty_uri(mock_s3_client):
    """Return None for empty URI."""
    result = bda_service.get_bda_result_json("")
    assert result is None


def test_get_bda_result_json_exception(mock_s3_client):
    """Return None when S3 read fails."""
    mock_s3_client.get_object.side_effect = Exception("S3 error")

    result = bda_service.get_bda_result_json("s3://bucket/key")

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


def test_extract_bda_output_s3_uri_custom_path(mock_s3_client):
    """Extract custom output path from job metadata."""
    mock_body = MagicMock()
    mock_body.read.return_value = b"""{
        "output_metadata": [{
            "segment_metadata": [{
                "custom_output_path": "s3://bucket/custom/output.json"
            }]
        }]
    }"""
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result == "s3://bucket/custom/output.json"


def test_extract_bda_output_s3_uri_standard_path(mock_s3_client):
    """Extract standard output path from job metadata."""
    mock_body = MagicMock()
    mock_body.read.return_value = b"""{
        "output_metadata": [{
            "segment_metadata": [{
                "standard_output_path": "s3://bucket/standard/output.json"
            }]
        }]
    }"""
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result == "s3://bucket/standard/output.json"


def test_extract_bda_output_s3_uri_no_path(mock_s3_client):
    """Return None when no output path found."""
    mock_body = MagicMock()
    mock_body.read.return_value = b'{"output_metadata": []}'
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result is None


def test_extract_bda_output_s3_uri_malformed(mock_s3_client):
    """Return None when metadata is malformed."""
    mock_body = MagicMock()
    mock_body.read.return_value = b'{"output_metadata": "not a list"}'
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    result = bda_service.extract_bda_output_s3_uri("bucket", "metadata.json")

    assert result is None
