from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from documentai_api.app import (
    JobStatus,
    _get_job_status,
    get_v1_document_processing_results,
    upload_document_for_processing,
)


def test_health(api_client):
    response = api_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"message": "healthy"}


def test_config(api_client):
    response = api_client.get("/config")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "supportedFileTypes" in data


def test_root(api_client):
    response = api_client.get("/")
    assert response.status_code == 200
    assert "status" in response.json()


def test_document_upload_no_file(api_client):
    response = api_client.post("/v1/documents")
    assert response.status_code == 422


def test_document_status_not_found(api_client):
    with patch("documentai_api.app.get_ddb_by_job_id") as mock_get_ddb:
        mock_get_ddb.return_value = None
        response = api_client.get("/v1/documents/fake-job-id")
        assert response.status_code == 404


def test_get_job_status_found():
    """Test _get_job_status when job exists."""
    with patch("documentai_api.app.get_ddb_by_job_id") as mock_get_ddb:
        mock_get_ddb.return_value = {
            "fileName": "test.pdf",
            "processStatus": "success",
            "v1ApiResponseJson": '{"status": "success"}',
        }

        result = _get_job_status("job-123")

    assert result.object_key == "test.pdf"
    assert result.process_status == "success"
    assert result.v1_response_json == '{"status": "success"}'


def test_get_job_status_not_found():
    """Test _get_job_status when job doesn't exist."""
    with patch("documentai_api.app.get_ddb_by_job_id") as mock_get_ddb:
        mock_get_ddb.return_value = None

        result = _get_job_status("job-123")

    assert result.ddb_record is None
    assert result.object_key is None
    assert result.process_status is None
    assert result.v1_response_json is None


@pytest.mark.asyncio
async def test_upload_document_for_processing_success():
    """Test successful document upload."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with (
        patch("documentai_api.app.s3_service.upload_file") as mock_upload,
    ):
        from documentai_api.config.constants import DocumentCategory

        await upload_document_for_processing(
            file=mock_file,
            unique_file_name="test.pdf",
            content_type="application/pdf",
            user_provided_document_category=DocumentCategory.INCOME,
            job_id="job-123",
            trace_id="trace-456",
            s3_location="s3://test-bucket",
        )

    mock_upload.assert_called_once()


@pytest.mark.asyncio
async def test_upload_document_for_processing_no_input_location():
    """Test upload fails when s3_location not set."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with pytest.raises(ValueError, match="S3 location not configured"):
        await upload_document_for_processing(
            file=mock_file,
            unique_file_name="test.pdf",
            content_type="application/pdf",
            s3_location=None,
        )


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_success():
    """Test polling returns results when processing completes."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        mock_get_job_status.return_value = JobStatus(
            ddb_record={"fileName": "test.pdf"},
            object_key="test.pdf",
            process_status="success",
            v1_response_json='{"status": "success", "data": {}}',
        )

        result = await get_v1_document_processing_results("job-123", timeout=10)

    assert result == {"status": "success", "data": {}}


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_timeout():
    """Test polling timeout with object_key."""
    with (
        patch("documentai_api.app._get_job_status") as mock_get_job_status,
        patch("documentai_api.app.classify_as_failed") as mock_classify_as_failed,
    ):
        mock_get_job_status.return_value = JobStatus(
            ddb_record={"fileName": "test.pdf"},
            object_key="test.pdf",
            process_status="started",
            v1_response_json=None,
        )
        mock_classify_as_failed.return_value = {"status": "failed", "message": "timeout"}

        result = await get_v1_document_processing_results("job-123", timeout=1)

    mock_classify_as_failed.assert_called_once()
    assert result["status"] == "failed"


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_timeout_no_object_key():
    """Test polling timeout without object_key."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        mock_get_job_status.return_value = JobStatus(
            ddb_record=None,
            object_key=None,
            process_status=None,
            v1_response_json=None,
        )

        result = await get_v1_document_processing_results("job-123", timeout=1)

    assert result["status"] == "failed"
    assert "timeout" in result["message"]


def test_get_document_results_with_extracted_data(api_client):
    """Test getting results with extracted data."""
    with (
        patch("documentai_api.app._get_job_status") as mock_get_job_status,
        patch(
            "documentai_api.utils.response_builder.build_v1_api_response"
        ) as mock_build_api_response,
    ):
        mock_get_job_status.return_value = JobStatus(
            ddb_record={"fileName": "test.pdf"},
            object_key="test.pdf",
            process_status="success",
            v1_response_json='{"status": "success"}',
        )
        mock_build_api_response.return_value = {"status": "success", "extractedData": {}}

        response = api_client.get("/v1/documents/job-123?include_extracted_data=true")

    assert response.status_code == 200
    mock_build_api_response.assert_called_once_with(
        object_key="test.pdf",
        status="success",
        include_extracted_data=True,
    )


def test_get_document_results_in_progress(api_client):
    """Test getting results for in-progress job."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        mock_get_job_status.return_value = JobStatus(
            ddb_record={"fileName": "test.pdf"},
            object_key="test.pdf",
            process_status="started",
            v1_response_json=None,
        )

        response = api_client.get("/v1/documents/job-123")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert "in progress" in data["message"].lower()


def test_list_schemas(api_client):
    """Test listing all schemas."""
    with patch("documentai_api.app.get_all_schemas") as mock_get_schemas:
        mock_get_schemas.return_value = {"type1": {}, "type2": {}}

        response = api_client.get("/v1/schemas")

    assert response.status_code == 200
    assert "schemas" in response.json()


def test_get_schema_found(api_client):
    """Test getting specific schema."""
    with patch("documentai_api.app.get_document_schema") as mock_get_schema:
        mock_get_schema.return_value = {"fields": []}

        response = api_client.get("/v1/schemas/invoice")

    assert response.status_code == 200


def test_get_schema_not_found(api_client):
    """Test getting non-existent schema."""
    with patch("documentai_api.app.get_document_schema") as mock_get_schema:
        mock_get_schema.return_value = None

        response = api_client.get("/v1/schemas/invalid")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_upload_document_for_processing_s3_failure():
    """Test S3 upload failure raises HTTPException."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with (
        patch("documentai_api.app.s3_service.upload_file") as mock_upload,
    ):
        mock_upload.side_effect = Exception("S3 error")

        with pytest.raises(HTTPException) as exc_info:
            await upload_document_for_processing(
                file=mock_file,
                unique_file_name="test.pdf",
                content_type="application/pdf",
                s3_location="s3://test-bucket",
            )

    assert exc_info.value.status_code == 500
    assert "upload failed" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_upload_document_for_processing_invalid_category_type():
    """Test invalid document category type raises ValueError."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with (
        pytest.raises(HTTPException),
    ):
        await upload_document_for_processing(
            file=mock_file,
            unique_file_name="test.pdf",
            content_type="application/pdf",
            user_provided_document_category="invalid_string",  # should be enum
            s3_location="s3://test-bucket",
        )


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_polling_error():
    """Test polling continues after DDB errors."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        # first call raises exception, second call returns success
        mock_get_job_status.side_effect = [
            Exception("DDB error"),
            JobStatus(
                ddb_record={"fileName": "test.pdf"},
                object_key="test.pdf",
                process_status="success",
                v1_response_json='{"status": "success"}',
            ),
        ]

        result = await get_v1_document_processing_results("job-123", timeout=10)

    assert result == {"status": "success"}


def test_create_document_invalid_file_type(api_client):
    """Test document upload with invalid file type."""
    with patch("documentai_api.app.magic.from_buffer") as mock_magic:
        mock_magic.return_value = "application/zip"  # unsupported type

        files = {"file": ("test.zip", b"fake zip content", "application/zip")}
        response = api_client.post("/v1/documents", files=files)

    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]


def test_create_document_asynchronous(api_client):
    """Test asynchronous document upload (default behavior, returns job_id immediately)."""
    with (
        patch("documentai_api.app.magic.from_buffer") as mock_magic,
        patch("documentai_api.app.upload_document_for_processing"),
    ):
        mock_magic.return_value = "application/pdf"

        files = {"file": ("test.pdf", b"fake pdf", "application/pdf")}
        response = api_client.post("/v1/documents", files=files)

    assert response.status_code == 200
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "not_started"
    assert "uploaded successfully" in data["message"].lower()


def test_create_document_synchronous(api_client):
    """Test synchronous document upload (wait=true)."""
    with (
        patch("documentai_api.app.magic.from_buffer") as mock_magic,
        patch("documentai_api.app.upload_document_for_processing"),
        patch("documentai_api.app.get_v1_document_processing_results") as mock_get_results,
    ):
        mock_magic.return_value = "application/pdf"
        mock_get_results.return_value = {"status": "success", "data": {}}

        files = {"file": ("test.pdf", b"fake pdf", "application/pdf")}
        response = api_client.post("/v1/documents?wait=true", files=files)

    assert response.status_code == 200
    assert response.json()["status"] == "success"


def test_get_document_results_error_handling(api_client):
    """Test error handling in get_document_results."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        mock_get_job_status.side_effect = Exception("Unexpected error")

        response = api_client.get("/v1/documents/job-123")

    assert response.status_code == 500
    assert "Failed to retrieve results" in response.json()["detail"]
