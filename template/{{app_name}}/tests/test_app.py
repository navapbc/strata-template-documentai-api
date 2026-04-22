import pytest
from fastapi import HTTPException

from documentai_api.app import (
    JobStatus,
    _get_job_status,
    app,
    get_v1_document_processing_results,
    upload_document_for_processing,
    verify_api_key,
)
from documentai_api.models.api_responses import JobStatusResponse


def mock_verify_api_key():
    """Mock API key verification - always passes."""
    return None


@pytest.fixture(autouse=True)
def disable_auth():
    """Disable API key authentication for all tests in this file."""
    app.dependency_overrides[verify_api_key] = mock_verify_api_key
    yield
    app.dependency_overrides.clear()


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


def test_document_status_not_found(ddb_doc_metadata_table_resource, api_client):
    response = api_client.get("/v1/documents/fake-job-id")
    assert response.status_code == 404


def test_get_job_status_found(ddb_doc_metadata_table):
    """Test _get_job_status when job exists."""
    from documentai_api.config.constants import ProcessStatus
    from documentai_api.schemas.document_metadata import DocumentMetadata

    ddb_record = {
        DocumentMetadata.FILE_NAME: "test.pdf",
        DocumentMetadata.JOB_ID: "test-job-id",
        DocumentMetadata.PROCESS_STATUS: ProcessStatus.SUCCESS.value,
        DocumentMetadata.V1_API_RESPONSE_JSON: '{"jobStatus": "success"}',
    }

    ddb_doc_metadata_table.put_item(Item=ddb_record)

    result = _get_job_status("test-job-id")

    assert result.object_key == "test.pdf"
    assert result.process_status == "success"
    assert result.v1_response_json == '{"jobStatus": "success"}'


def test_get_job_status_not_found(ddb_doc_metadata_table):
    """Test _get_job_status when job doesn't exist."""
    result = _get_job_status("test-job-id")

    assert result.ddb_record is None
    assert result.object_key is None
    assert result.process_status is None
    assert result.v1_response_json is None


@pytest.mark.asyncio
async def test_upload_document_for_processing_success(
    runtime_required_env, blank_pdf_file, s3_bucket, mocker
):
    """Test successful document upload."""
    from documentai_api.config.constants import DocumentCategory

    await upload_document_for_processing(
        file=blank_pdf_file.open("rb"),
        original_file_name="test.pdf",
        unique_file_name="test-unique.pdf",
        content_type="application/pdf",
        user_provided_document_category=DocumentCategory.INCOME,
        job_id="test-job-id",
        trace_id="test-trace-id",
    )

    uploaded_file_in_s3 = s3_bucket.Object("input/test-unique.pdf")
    assert uploaded_file_in_s3.content_type == "application/pdf"


@pytest.mark.asyncio
async def test_upload_document_for_processing_no_env(mocker):
    """Test upload fails when DOCUMENTAI_INPUT_LOCATION not set."""
    mock_file = mocker.MagicMock()

    with (
        pytest.raises(ValueError, match="DOCUMENTAI_INPUT_LOCATION environment variable not set"),
    ):
        await upload_document_for_processing(
            file=mock_file,
            original_file_name="test.pdf",
            unique_file_name="test.pdf",
            content_type="application/pdf",
        )


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_success(mocker):
    """Test polling returns results when processing completes."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    mock_get_job_status.return_value = JobStatus(
        ddb_record={"fileName": "test.pdf"},
        object_key="test.pdf",
        process_status="success",
        v1_response_json='{"jobId": "test-job-id", "jobStatus": "success", "message": "Document processed successfully"}',
    )

    result = await get_v1_document_processing_results("test-job-id", timeout=10)

    assert result.job_status == "success"


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_timeout(mocker):
    """Test polling timeout with object_key."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    mock_get_job_status.return_value = JobStatus(
        ddb_record={"fileName": "test.pdf"},
        object_key="test.pdf",
        process_status="started",
        v1_response_json=None,
    )

    mock_classify_as_failed = mocker.patch("documentai_api.app.classify_as_failed")
    mock_classify_as_failed.return_value = {
        "jobId": "test-job-id",
        "jobStatus": "failed",
        "message": "timeout",
    }

    result = await get_v1_document_processing_results("test-job-id", timeout=1)

    mock_classify_as_failed.assert_called_once()
    assert result.job_status == "failed"


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_timeout_no_object_key(mocker):
    """Test polling timeout without object_key."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    mock_get_job_status.return_value = JobStatus(
        ddb_record=None,
        object_key=None,
        process_status=None,
        v1_response_json=None,
    )

    result = await get_v1_document_processing_results("test-job-id", timeout=1)

    assert result.job_status == "failed"
    assert "timeout" in result.message


def test_get_document_results_with_extracted_data(api_client, mocker):
    """Test getting results with extracted data."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    mock_get_job_status.return_value = JobStatus(
        ddb_record={"fileName": "test.pdf"},
        object_key="test.pdf",
        process_status="success",
        v1_response_json='{"jobId": "test-job-id", "jobStatus": "success", "message": "Document processed successfully"}',
    )

    mock_build_api_response = mocker.patch(
        "documentai_api.utils.response_builder.build_v1_api_response"
    )
    mock_build_api_response.return_value = {
        "jobId": "test-job-id",
        "jobStatus": "success",
        "message": "Document processed successfully",
        "extractedData": {},
    }

    response = api_client.get("/v1/documents/test-job-id?include_extracted_data=true")

    assert response.status_code == 200
    mock_build_api_response.assert_called_once_with(
        object_key="test.pdf",
        job_status="success",
        include_extracted_data=True,
    )


def test_get_document_results_in_progress(api_client, mocker):
    """Test getting results for in-progress job."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    mock_get_job_status.return_value = JobStatus(
        ddb_record={"fileName": "test.pdf"},
        object_key="test.pdf",
        process_status="started",
        v1_response_json=None,
    )

    response = api_client.get("/v1/documents/test-job-id")

    assert response.status_code == 200
    data = response.json()
    assert data["jobStatus"] == "started"
    assert "in progress" in data["message"].lower()


def test_list_schemas(api_client, mocker):
    """Test listing all schemas."""
    mock_get_schemas = mocker.patch("documentai_api.app.get_all_schemas")
    mock_get_schemas.return_value = {"type1": {}, "type2": {}}

    response = api_client.get("/v1/schemas")

    assert response.status_code == 200
    assert "schemas" in response.json()


def test_get_schema_found(api_client, mocker):
    """Test getting specific schema."""
    mock_get_schema = mocker.patch("documentai_api.app.get_document_schema")
    mock_get_schema.return_value = {"documentType": "invoice", "fields": []}

    response = api_client.get("/v1/schemas/invoice")

    assert response.status_code == 200


def test_get_schema_not_found(api_client, mocker):
    """Test getting non-existent schema."""
    mock_get_schema = mocker.patch("documentai_api.app.get_document_schema")
    mock_get_schema.return_value = None

    response = api_client.get("/v1/schemas/invalid")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_upload_document_for_processing_s3_failure(blank_pdf_file, s3_bucket, monkeypatch):
    """Test S3 upload failure raises HTTPException."""
    monkeypatch.setenv("DOCUMENTAI_INPUT_LOCATION", f"s3://{s3_bucket.name}-foo/input")

    with pytest.raises(HTTPException) as exc_info:
        await upload_document_for_processing(
            file=blank_pdf_file,
            original_file_name="test.pdf",
            unique_file_name="test.pdf",
            content_type="application/pdf",
        )

    assert exc_info.value.status_code == 500
    assert "upload failed" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_upload_document_for_processing_invalid_category_type(
    blank_pdf_file, runtime_required_env
):
    """Test invalid document category type raises ValueError."""
    with pytest.raises(HTTPException):
        await upload_document_for_processing(
            file=blank_pdf_file,
            original_file_name="test.pdf",
            unique_file_name="test.pdf",
            content_type="application/pdf",
            user_provided_document_category="invalid_string",  # should be enum
        )


@pytest.mark.asyncio
async def test_get_v1_document_processing_results_polling_error(mocker):
    """Test polling continues after DDB errors."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    # first call raises exception, second call returns success
    mock_get_job_status.side_effect = [
        Exception("DDB error"),
        JobStatus(
            ddb_record={"fileName": "test.pdf"},
            object_key="test.pdf",
            process_status="success",
            v1_response_json='{"jobId": "test-job-id", "jobStatus": "success", "message": "Document processed successfully"}',
        ),
    ]

    result = await get_v1_document_processing_results("test-job-id", timeout=10)

    assert result.job_status == "success"


def test_create_document_invalid_file_type(api_client, empty_zip_bytes):
    """Test document upload with invalid file type."""
    files = {"file": ("test.zip", empty_zip_bytes, "application/zip")}
    response = api_client.post("/v1/documents", files=files)

    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]


def test_create_document_asynchronous(api_client, blank_pdf_bytes):
    """Test asynchronous document upload (default behavior, returns job_id immediately)."""
    files = {"file": ("test.pdf", blank_pdf_bytes, "application/pdf")}
    response = api_client.post("/v1/documents", files=files)

    assert response.status_code == 200
    data = response.json()
    assert "jobId" in data
    assert data["jobStatus"] == "not_started"
    assert "uploaded successfully" in data["message"].lower()


def test_create_document_synchronous(api_client, blank_pdf_bytes, mocker):
    """Test synchronous document upload (wait=true)."""
    mock_get_results = mocker.patch("documentai_api.app.get_v1_document_processing_results")
    mock_get_results.return_value = JobStatusResponse(
        job_id="test-id", job_status="success", message="Document processed successfully"
    )

    files = {"file": ("test.pdf", blank_pdf_bytes, "application/pdf")}
    response = api_client.post("/v1/documents?wait=true", files=files)

    assert response.status_code == 200
    assert response.json()["jobStatus"] == "success"


def test_get_document_results_error_handling(api_client, mocker):
    """Test error handling in get_document_results."""
    mock_get_job_status = mocker.patch("documentai_api.app._get_job_status")
    mock_get_job_status.side_effect = Exception("Unexpected error")

    response = api_client.get("/v1/documents/test-job-id")

    assert response.status_code == 500
    assert "Failed to retrieve results" in response.json()["detail"]
