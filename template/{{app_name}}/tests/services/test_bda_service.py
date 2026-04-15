"""Tests for services/bda.py."""

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


def test_get_bda_result_json_success(s3_bucket):
    """Read BDA result JSON from S3."""
    s3_bucket.put_object(Key="path/to/result.json", Body=b'{"result": "success"}')

    result = bda_service.get_bda_result_json(f"s3://{s3_bucket.name}/path/to/result.json")

    assert result == {"result": "success"}


def test_get_bda_result_json_empty_uri():
    """Return None for empty URI."""
    result = bda_service.get_bda_result_json("")
    assert result is None


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


def test_extract_bda_output_s3_uri_custom_path(s3_bucket):
    """Extract custom output path from job metadata."""
    s3_bucket.put_object(
        Key="metadata.json",
        Body=f'{{"output_metadata": [{{"segment_metadata": [{{"custom_output_path": "s3://{s3_bucket.name}/custom/output.json"}}]}}]}}'.encode(),
    )

    result = bda_service.extract_bda_output_s3_uri(s3_bucket.name, "metadata.json")

    assert result == f"s3://{s3_bucket.name}/custom/output.json"


def test_extract_bda_output_s3_uri_standard_path(s3_bucket):
    """Extract standard output path from job metadata."""
    s3_bucket.put_object(
        Key="metadata.json",
        Body=f'{{"output_metadata": [{{"segment_metadata": [{{"standard_output_path": "s3://{s3_bucket.name}/standard/output.json"}}]}}]}}'.encode(),
    )

    result = bda_service.extract_bda_output_s3_uri(s3_bucket.name, "metadata.json")

    assert result == f"s3://{s3_bucket.name}/standard/output.json"


def test_extract_bda_output_s3_uri_no_path(s3_bucket):
    """Return None when no output path found."""
    s3_bucket.put_object(Key="metadata.json", Body=b'{"output_metadata": []}')

    result = bda_service.extract_bda_output_s3_uri(s3_bucket.name, "metadata.json")
    assert result is None


def test_extract_bda_output_s3_uri_malformed(s3_bucket):
    """Return None when metadata is malformed."""
    s3_bucket.put_object(Key="metadata.json", Body=b'{"output_metadata": "not a list"}')

    result = bda_service.extract_bda_output_s3_uri(s3_bucket.name, "metadata.json")

    assert result is None
