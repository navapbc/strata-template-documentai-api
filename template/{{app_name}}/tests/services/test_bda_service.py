"""Tests for services/bda.py."""

from moto import mock_aws

from documentai_api.services import bda as bda_service


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
def test_get_bda_result_json_success(s3_bucket):
    """Read BDA result JSON from S3."""
    s3_bucket.put_object(
        Bucket="test-bucket", Key="path/to/result.json", Body=b'{"result": "success"}'
    )

    result = bda_service.get_bda_result_json("s3://test-bucket/path/to/result.json")

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
def test_extract_bda_output_s3_uri_custom_path(s3_bucket):
    """Extract custom output path from job metadata."""
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="metadata.json",
        Body=b"""{"output_metadata": [{"segment_metadata": [{"custom_output_path": "s3://test-bucket/custom/output.json"}]}]}""",
    )

    result = bda_service.extract_bda_output_s3_uri("test-bucket", "metadata.json")

    assert result == "s3://test-bucket/custom/output.json"


@mock_aws
def test_extract_bda_output_s3_uri_standard_path(s3_bucket):
    """Extract standard output path from job metadata."""
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="metadata.json",
        Body=b"""{"output_metadata": [{"segment_metadata": [{"standard_output_path": "s3://test-bucket/standard/output.json"}]}]}""",
    )

    result = bda_service.extract_bda_output_s3_uri("test-bucket", "metadata.json")

    assert result == "s3://test-bucket/standard/output.json"


@mock_aws
def test_extract_bda_output_s3_uri_no_path(s3_bucket):
    """Return None when no output path found."""
    s3_bucket.put_object(Bucket="test-bucket", Key="metadata.json", Body=b'{"output_metadata": []}')

    result = bda_service.extract_bda_output_s3_uri("test-bucket", "metadata.json")

    assert result is None


@mock_aws
def test_extract_bda_output_s3_uri_malformed(s3_bucket):
    """Return None when metadata is malformed."""
    s3_bucket.put_object(
        Bucket="test-bucket", Key="metadata.json", Body=b'{"output_metadata": "not a list"}'
    )

    result = bda_service.extract_bda_output_s3_uri("test-bucket", "metadata.json")

    assert result is None
