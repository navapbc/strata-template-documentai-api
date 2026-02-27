from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from documentai_api.app import (
    JobStatus,
    _get_job_status,
    app,
    get_v1_document_processing_results,
    upload_document_for_processing,
)
from documentai_api.utils.models import PageMetadata

client = TestClient(app)


@pytest.fixture
def mock_multipage_upload(monkeypatch):
    """Mock common multipage upload dependencies."""
    monkeypatch.setenv("DOCUMENTAI_MULTIPAGE_UPLOAD_SESSIONS_TABLE_NAME", "test-multipage-table")

    with (
        patch("documentai_api.app.magic.from_buffer") as mock_magic,
        patch("documentai_api.app.multipage_page_exists") as mock_page_exists,
        patch("documentai_api.app.upload_document_for_processing") as mock_upload,
        patch("documentai_api.app.upsert_multipage_session") as mock_upsert,
    ):
        mock_magic.return_value = "application/pdf"
        mock_page_exists.return_value = False

        yield {
            "magic": mock_magic,
            "page_exists": mock_page_exists,
            "upload": mock_upload,
            "upsert": mock_upsert,
        }


@pytest.fixture
def mock_multipage_submit(monkeypatch):
    """Mock common multipage submit dependencies."""
    monkeypatch.setenv("DOCUMENTAI_MULTIPAGE_UPLOAD_SESSIONS_TABLE_NAME", "test-multipage-table")

    with (
        patch("documentai_api.utils.ddb.is_multipage_session_submitted") as mock_is_submitted,
        patch("documentai_api.utils.ddb.get_multipage_session_pages") as mock_get_pages,
        patch("documentai_api.utils.pdf.merge_pages_to_pdf") as mock_merge,
        patch("documentai_api.app.upload_document_for_processing") as mock_upload,
        patch("documentai_api.utils.files.create_upload_file_from_bytes") as mock_create_file,
        patch("documentai_api.utils.ddb.mark_multipage_session_submitted") as mock_mark_submitted,
    ):
        mock_merge.return_value = b"merged pdf bytes"
        mock_is_submitted.return_value = False  # default to not submitted

        yield {
            "is_submitted": mock_is_submitted,
            "get_pages": mock_get_pages,
            "merge": mock_merge,
            "upload": mock_upload,
            "create_file": mock_create_file,
            "mark_submitted": mock_mark_submitted,
        }


def create_page_metadata(
    page_number: int, session_id: str = "session-123", category: str | None = None
) -> PageMetadata:
    """Helper to create PageMetadata for tests."""
    return PageMetadata(
        page_number=page_number,
        s3_key=f"pending_merge/{session_id}/page-{page_number}.pdf",
        s3_bucket_name="test-bucket",
        category=category,
    )


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"message": "healthy"}


def test_config():
    response = client.get("/config")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "supportedFileTypes" in data


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "status" in response.json()


def test_document_upload_no_file():
    response = client.post("/v1/documents")
    assert response.status_code == 422


def test_document_status_not_found():
    with patch("documentai_api.app.get_ddb_by_job_id") as mock_get_ddb:
        mock_get_ddb.return_value = None
        response = client.get("/v1/documents/fake-job-id")
        assert response.status_code == 404


@pytest.mark.parametrize(
    ("ddb_return", "expected_values"),
    [
        (
            {
                "fileName": "test.pdf",
                "processStatus": "success",
                "v1ApiResponseJson": '{"status": "success"}',
            },
            ("test.pdf", "success", '{"status": "success"}'),
        ),
        (None, (None, None, None)),
    ],
)
def test_get_job_status(ddb_return, expected_values):
    """Test _get_job_status."""
    with patch("documentai_api.app.get_ddb_by_job_id") as mock_get_ddb:
        mock_get_ddb.return_value = ddb_return
        result = _get_job_status("job-123")
    assert result.object_key == expected_values[0]
    assert result.process_status == expected_values[1]
    assert result.v1_response_json == expected_values[2]


@pytest.mark.asyncio
async def test_upload_document_for_processing_success():
    """Test successful document upload."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with (
        patch("documentai_api.app.DDE_INPUT_LOCATION", "s3://test-bucket"),
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
        )

    mock_upload.assert_called_once()


@pytest.mark.asyncio
async def test_upload_document_for_processing_no_env():
    """Test upload fails when DDE_INPUT_LOCATION not set."""
    mock_file = MagicMock()

    with (
        patch("documentai_api.app.DDE_INPUT_LOCATION", None),
        pytest.raises(ValueError, match="DDE_INPUT_LOCATION"),
    ):
        await upload_document_for_processing(
            file=mock_file,
            unique_file_name="test.pdf",
            content_type="application/pdf",
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


def test_get_document_results_with_extracted_data():
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

        response = client.get("/v1/documents/job-123?include_extracted_data=true")

    assert response.status_code == 200
    mock_build_api_response.assert_called_once_with(
        object_key="test.pdf",
        status="success",
        include_extracted_data=True,
    )


def test_get_document_results_in_progress():
    """Test getting results for in-progress job."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        mock_get_job_status.return_value = JobStatus(
            ddb_record={"fileName": "test.pdf"},
            object_key="test.pdf",
            process_status="started",
            v1_response_json=None,
        )

        response = client.get("/v1/documents/job-123")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "started"
    assert "in progress" in data["message"].lower()


def test_list_schemas():
    """Test listing all schemas."""
    with patch("documentai_api.app.get_all_schemas") as mock_get_schemas:
        mock_get_schemas.return_value = {"type1": {}, "type2": {}}

        response = client.get("/v1/schemas")

    assert response.status_code == 200
    assert "schemas" in response.json()


@pytest.mark.parametrize(
    ("schema_return", "expected_status"),
    [
        ({"fields": []}, 200),  # found
        (None, 404),  # not found
    ],
)
def test_get_schema(schema_return, expected_status):
    """Test getting specific schema."""
    with patch("documentai_api.app.get_document_schema") as mock_get_schema:
        mock_get_schema.return_value = schema_return
        response = client.get("/v1/schemas/invoice")
    assert response.status_code == expected_status


@pytest.mark.asyncio
async def test_upload_document_for_processing_s3_failure():
    """Test S3 upload failure raises HTTPException."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with (
        patch("documentai_api.app.DDE_INPUT_LOCATION", "s3://test-bucket"),
        patch("documentai_api.app.s3_service.upload_file") as mock_upload,
    ):
        mock_upload.side_effect = Exception("S3 error")

        with pytest.raises(HTTPException) as exc_info:
            await upload_document_for_processing(
                file=mock_file,
                unique_file_name="test.pdf",
                content_type="application/pdf",
            )

    assert exc_info.value.status_code == 500
    assert "upload failed" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_upload_document_for_processing_invalid_category_type():
    """Test invalid document category type raises ValueError."""
    mock_file = MagicMock()
    mock_file.file = MagicMock()

    with (
        patch("documentai_api.app.DDE_INPUT_LOCATION", "s3://test-bucket"),
        pytest.raises(HTTPException),
    ):
        await upload_document_for_processing(
            file=mock_file,
            unique_file_name="test.pdf",
            content_type="application/pdf",
            user_provided_document_category="invalid_string",  # should be enum
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


def test_create_document_invalid_file_type():
    """Test document upload with invalid file type."""
    with patch("documentai_api.app.magic.from_buffer") as mock_magic:
        mock_magic.return_value = "application/zip"  # unsupported type

        files = {"file": ("test.zip", b"fake zip content", "application/zip")}
        response = client.post("/v1/documents", files=files)

    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]


def test_create_document_asynchronous():
    """Test asynchronous document upload (default behavior, returns job_id immediately)."""
    with (
        patch("documentai_api.app.magic.from_buffer") as mock_magic,
        patch("documentai_api.app.upload_document_for_processing"),
    ):
        mock_magic.return_value = "application/pdf"

        files = {"file": ("test.pdf", b"fake pdf", "application/pdf")}
        response = client.post("/v1/documents", files=files)

    assert response.status_code == 200
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "not_started"
    assert "uploaded successfully" in data["message"].lower()


def test_create_document_synchronous():
    """Test synchronous document upload (wait=true)."""
    with (
        patch("documentai_api.app.magic.from_buffer") as mock_magic,
        patch("documentai_api.app.upload_document_for_processing"),
        patch("documentai_api.app.get_v1_document_processing_results") as mock_get_results,
    ):
        mock_magic.return_value = "application/pdf"
        mock_get_results.return_value = {"status": "success", "data": {}}

        files = {"file": ("test.pdf", b"fake pdf", "application/pdf")}
        response = client.post("/v1/documents?wait=true", files=files)

    assert response.status_code == 200
    assert response.json()["status"] == "success"


def test_get_document_results_error_handling():
    """Test error handling in get_document_results."""
    with patch("documentai_api.app._get_job_status") as mock_get_job_status:
        mock_get_job_status.side_effect = Exception("Unexpected error")

        response = client.get("/v1/documents/job-123")

    assert response.status_code == 500
    assert "Failed to retrieve results" in response.json()["detail"]


@pytest.mark.parametrize(
    ("session_id", "page_number", "expected_session"),
    [
        (None, 1, None),  # new session - sessionId will be generated
        ("session-123", 2, "session-123"),  # existing session
    ],
)
def test_upload_multipage_page_sessions(
    multipage_ddb_table, mock_multipage_upload, session_id, page_number, expected_session
):
    """Test uploading pages to new and existing sessions."""
    files = {"file": ("page.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": page_number}
    if session_id:
        data["session_id"] = session_id

    response = client.post("/v1/multipage/pages", files=files, data=data)

    assert response.status_code == 200
    result = response.json()
    assert "sessionId" in result
    if expected_session:
        assert result["sessionId"] == expected_session
    assert result["pageNumber"] == page_number
    assert "uploaded successfully" in result["message"].lower()


@pytest.mark.parametrize(
    ("file_type", "file_name"),
    [
        ("application/zip", "test.zip"),
        ("text/plain", "test.txt"),
        ("image/gif", "test.gif"),
    ],
)
def test_upload_multipage_page_invalid_file_type(
    multipage_ddb_table, mock_multipage_upload, file_type, file_name
):
    """Test multipage upload with invalid file types."""
    mock_multipage_upload["magic"].return_value = file_type

    files = {"file": (file_name, b"fake content", file_type)}
    data = {"page_number": 1}
    response = client.post("/v1/multipage/pages", files=files, data=data)

    assert response.status_code == 400
    assert "Invalid file type" in response.json()["detail"]


@pytest.mark.parametrize(
    ("page_exists", "overwrite", "expected_status"),
    [
        (True, False, 409),  # duplicate without overwrite -> conflict
        (True, True, 200),  # duplicate with overwrite -> success
        (False, False, 200),  # new page -> success
        (False, True, 200),  # new page with overwrite flag -> success
    ],
)
def test_upload_multipage_page_overwrite_scenarios(
    multipage_ddb_table, mock_multipage_upload, page_exists, overwrite, expected_status
):
    """Test multipage upload duplicate/overwrite scenarios."""
    mock_multipage_upload["page_exists"].return_value = page_exists

    files = {"file": ("page1.pdf", b"fake pdf", "application/pdf")}
    data = {"session_id": "session-123", "page_number": 1, "overwrite": overwrite}
    response = client.post("/v1/multipage/pages", files=files, data=data)

    assert response.status_code == expected_status
    if expected_status == 409:
        assert "already exists" in response.json()["detail"]


def test_upload_multipage_page_with_category(multipage_ddb_table, mock_multipage_upload):
    """Test multipage upload with document category."""
    files = {"file": ("page1.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": 1, "category": "income"}
    response = client.post("/v1/multipage/pages", files=files, data=data)

    assert response.status_code == 200
    mock_multipage_upload["upsert"].assert_called_once()


def test_submit_multipage_document_not_found(multipage_ddb_table, mock_multipage_submit):
    """Test submitting non-existent session."""
    mock_multipage_submit["get_pages"].return_value = []

    data = {"session_id": "nonexistent-session"}
    response = client.post("/v1/multipage/submit", data=data)

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_submit_multipage_document_synchronous(multipage_ddb_table, mock_multipage_submit):
    """Test synchronous multipage submission (wait=true)."""
    with patch("documentai_api.app.get_v1_document_processing_results") as mock_get_results:
        mock_multipage_submit["get_pages"].return_value = [
            create_page_metadata(1, category="income"),
        ]
        mock_get_results.return_value = {"status": "success", "data": {}}

        data = {"session_id": "session-123"}
        response = client.post("/v1/multipage/submit?wait=true", data=data)

    assert response.status_code == 200
    assert response.json()["status"] == "success"


def test_submit_multipage_document_with_category(multipage_ddb_table, mock_multipage_submit):
    """Test submit uses category from first page."""
    from documentai_api.config.constants import DocumentCategory

    mock_multipage_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]

    data = {"session_id": "session-123"}
    response = client.post("/v1/multipage/submit", data=data)

    assert response.status_code == 200
    mock_multipage_submit["upload"].assert_called_once()

    # verify category was converted to enum
    call_args = mock_multipage_submit["upload"].call_args
    assert call_args.kwargs["user_provided_document_category"] == DocumentCategory.INCOME


@pytest.mark.parametrize(
    ("mock_method", "error"),
    [
        ("merge", Exception("PDF merge failed")),
        ("upload", HTTPException(status_code=500, detail="Upload failed")),
    ],
)
def test_submit_multipage_document_errors(
    multipage_ddb_table, mock_multipage_submit, mock_method, error
):
    """Test error handling during multipage submit."""
    mock_multipage_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]
    mock_multipage_submit[mock_method].side_effect = error

    data = {"session_id": "session-123"}
    response = client.post("/v1/multipage/submit", data=data)

    assert response.status_code == 500


def test_submit_multipage_document_already_submitted(multipage_ddb_table, mock_multipage_submit):
    """Test submitting a session that was already submitted."""
    mock_multipage_submit["is_submitted"].return_value = True

    data = {"session_id": "session-123"}
    response = client.post("/v1/multipage/submit", data=data)

    assert response.status_code == 400
    assert "already been submitted" in response.json()["detail"]

    # verify we didn't try to process
    mock_multipage_submit["get_pages"].assert_not_called()
    mock_multipage_submit["merge"].assert_not_called()


def test_submit_multipage_document_success(multipage_ddb_table, mock_multipage_submit):
    """Test successful multipage document submission."""
    mock_multipage_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]

    data = {"session_id": "session-123"}
    response = client.post("/v1/multipage/submit", data=data)

    assert response.status_code == 200
    result = response.json()
    assert "jobId" in result
    assert result["sessionId"] == "session-123"
    assert result["status"] == "not_started"
    assert result["pageCount"] == 2

    # verify session was marked as submitted
    mock_multipage_submit["mark_submitted"].assert_called_once_with("session-123")


def test_upload_multipage_page_error_handling(multipage_ddb_table, mock_multipage_upload):
    """Test error handling during multipage page upload."""
    mock_multipage_upload["upload"].side_effect = Exception("S3 upload failed")

    files = {"file": ("page1.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": 1}
    response = client.post("/v1/multipage/pages", files=files, data=data)

    assert response.status_code == 500
    assert "Failed to upload page" in response.json()["detail"]


def test_get_multipage_session_success(multipage_ddb_table, mock_multipage_submit):
    """Test getting session details."""
    mock_multipage_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]

    response = client.get("/v1/multipage/sessions/session-123")

    assert response.status_code == 200
    result = response.json()
    assert result["sessionId"] == "session-123"
    assert result["pageCount"] == 2
    assert len(result["pages"]) == 2
    assert result["pages"][0]["pageNumber"] == 1
    assert result["pages"][0]["category"] == "income"


@pytest.mark.parametrize(
    ("mock_side_effect", "expected_status"),
    [
        (True, 204),  # success - return value
        (False, 404),  # not found - return value
        (
            ValueError("Cannot delete - session already submitted"),
            400,
        ),  # already submitted - exception
    ],
)
def test_delete_multipage_page(multipage_ddb_table, monkeypatch, mock_side_effect, expected_status):
    """Test deleting a page."""
    monkeypatch.setenv("DOCUMENTAI_MULTIPAGE_UPLOAD_SESSIONS_TABLE_NAME", "test-multipage-table")

    with patch("documentai_api.utils.ddb.delete_multipage_page") as mock_delete:
        if isinstance(mock_side_effect, Exception):
            mock_delete.side_effect = mock_side_effect
        else:
            mock_delete.return_value = mock_side_effect

        response = client.delete("/v1/multipage/sessions/session-123/pages/1")

        assert response.status_code == expected_status
        if expected_status == 400:
            assert "already" in response.json()["detail"]


@pytest.mark.parametrize(
    ("mock_side_effect", "expected_status"),
    [
        (True, 204),  # success
        (False, 404),  # not found
        (ValueError("Cannot delete - session already submitted"), 400),  # already submitted
    ],
)
def test_delete_multipage_session(
    multipage_ddb_table, monkeypatch, mock_side_effect, expected_status
):
    """Test deleting entire session."""
    monkeypatch.setenv("DOCUMENTAI_MULTIPAGE_UPLOAD_SESSIONS_TABLE_NAME", "test-multipage-table")

    with patch("documentai_api.utils.ddb.delete_multipage_session") as mock_delete:
        if isinstance(mock_side_effect, Exception):
            mock_delete.side_effect = mock_side_effect
        else:
            mock_delete.return_value = mock_side_effect

        response = client.delete("/v1/multipage/sessions/session-123")

        assert response.status_code == expected_status
        if expected_status == 400:
            assert "already" in response.json()["detail"]
