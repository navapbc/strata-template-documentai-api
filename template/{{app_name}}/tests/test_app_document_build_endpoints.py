"""Tests for document build endpoints."""

from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from documentai_api.app import app
from documentai_api.utils import env
from documentai_api.utils.models import PageMetadata

client = TestClient(app)


@pytest.fixture
def mock_document_build_upload(monkeypatch):
    """Mock common document build upload dependencies."""
    monkeypatch.setenv(env.DOCUMENTAI_BUILD_TABLE_NAME, "test-document-builds-table")

    with (
        patch("documentai_api.app.magic.from_buffer") as mock_magic,
        patch("documentai_api.app.document_build_page_exists") as mock_page_exists,
        patch("documentai_api.app.upload_document_for_processing") as mock_upload,
        patch("documentai_api.app.upsert_document_build_page") as mock_upsert,
        patch(
            "documentai_api.app.DOCUMENTAI_PREPROCESSING_LOCATION", "s3://test-bucket/preprocessing"
        ),
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
def mock_document_build_submit(monkeypatch):
    """Mock common document build submit dependencies."""
    monkeypatch.setenv(env.DOCUMENTAI_BUILD_TABLE_NAME, "test-document-builds-table")

    with (
        patch("documentai_api.utils.ddb.is_document_build_submitted") as mock_is_submitted,
        patch("documentai_api.utils.ddb.get_document_build_pages") as mock_get_pages,
        patch("documentai_api.utils.pdf.merge_pages_to_pdf") as mock_merge,
        patch("documentai_api.app.upload_document_for_processing") as mock_upload,
        patch("documentai_api.utils.files.create_upload_file_from_bytes") as mock_create_file,
        patch("documentai_api.utils.ddb.mark_document_build_submitted") as mock_mark_submitted,
    ):
        mock_merge.return_value = b"merged pdf bytes"
        mock_is_submitted.return_value = False

        yield {
            "is_submitted": mock_is_submitted,
            "get_pages": mock_get_pages,
            "merge": mock_merge,
            "upload": mock_upload,
            "create_file": mock_create_file,
            "mark_submitted": mock_mark_submitted,
        }


def test_create_build(document_build_ddb_table):
    with patch("documentai_api.app.create_document_build") as mock_create:
        mock_create.return_value = "fake-build-id"
        response = client.post("/v1/builds")

    assert response.status_code == 200
    result = response.json()
    assert "buildId" in result
    assert result["message"] == "Build created successfully"


def create_page_metadata(
    page_number: int, build_id: str = "test-build-id", category: str | None = None
) -> PageMetadata:
    """Helper to create PageMetadata for tests."""
    return PageMetadata(
        page_number=page_number,
        s3_key=f"builds/{build_id}/page-{page_number}.pdf",
        s3_bucket_name="test-bucket",
        category=category,
    )


@pytest.mark.parametrize(
    ("build_id", "page_number", "expected_build"),
    [
        (None, 1, None),  # new build - buildId will be generated
        ("test-build-id", 2, "test-build-id"),  # existing build
    ],
)
def test_upload_document_build_page_builds(
    document_build_ddb_table, mock_document_build_upload, build_id, page_number, expected_build
):
    """Test uploading pages to new and existing builds."""
    files = {"file": ("page.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": page_number}
    response = client.post("/v1/builds/test-build-id/pages", files=files, data=data)

    assert response.status_code == 200
    result = response.json()
    assert "buildId" in result
    if expected_build:
        assert result["buildId"] == expected_build
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
def test_upload_document_build_page_invalid_file_type(
    document_build_ddb_table, mock_document_build_upload, file_type, file_name
):
    """Test document build upload with invalid file types."""
    mock_document_build_upload["magic"].return_value = file_type

    files = {"file": (file_name, b"fake content", file_type)}
    data = {"page_number": 1}
    response = client.post("/v1/builds/test-build-id/pages", files=files, data=data)

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
def test_upload_document_build_page_overwrite_scenarios(
    document_build_ddb_table, mock_document_build_upload, page_exists, overwrite, expected_status
):
    """Test document build upload duplicate/overwrite scenarios."""
    mock_document_build_upload["page_exists"].return_value = page_exists

    files = {"file": ("page1.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": 1, "overwrite": overwrite}
    response = client.post("/v1/builds/test-build-id/pages", files=files, data=data)

    assert response.status_code == expected_status
    if expected_status == 409:
        assert "already exists" in response.json()["detail"]


def test_upload_document_build_page_with_category(
    document_build_ddb_table, mock_document_build_upload
):
    """Test document build upload with document category."""
    files = {"file": ("page1.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": 1, "category": "income"}
    response = client.post("/v1/builds/test-build-id/pages", files=files, data=data)

    assert response.status_code == 200
    mock_document_build_upload["upsert"].assert_called_once()


def test_submit_document_build_not_found(document_build_ddb_table, mock_document_build_submit):
    """Test submitting non-existent build."""
    mock_document_build_submit["get_pages"].return_value = []

    response = client.post("/v1/builds/nonexistent-build/submit")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_submit_document_build_synchronous(document_build_ddb_table, mock_document_build_submit):
    """Test synchronous document build submission (wait=true)."""
    with patch("documentai_api.app.get_v1_document_processing_results") as mock_get_results:
        mock_document_build_submit["get_pages"].return_value = [
            create_page_metadata(1, category="income"),
        ]
        mock_get_results.return_value = {"status": "success", "data": {}}

        response = client.post("/v1/builds/test-build-id/submit?wait=true")

    assert response.status_code == 200
    assert response.json()["status"] == "success"


def test_submit_document_build_with_category(document_build_ddb_table, mock_document_build_submit):
    """Test submit uses category from first page."""
    from documentai_api.config.constants import DocumentCategory

    mock_document_build_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]

    response = client.post("/v1/builds/test-build-id/submit")

    assert response.status_code == 200
    mock_document_build_submit["upload"].assert_called_once()

    # verify category was converted to enum
    call_args = mock_document_build_submit["upload"].call_args
    assert call_args.kwargs["user_provided_document_category"] == DocumentCategory.INCOME


@pytest.mark.parametrize(
    ("mock_method", "error"),
    [
        ("merge", Exception("PDF merge failed")),
        ("upload", HTTPException(status_code=500, detail="Upload failed")),
    ],
)
def test_submit_document_build_errors(
    document_build_ddb_table, mock_document_build_submit, mock_method, error
):
    """Test error handling during document build submit."""
    mock_document_build_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]
    mock_document_build_submit[mock_method].side_effect = error

    response = client.post("/v1/builds/test-build-id/submit")

    assert response.status_code == 500


def test_submit_document_build_already_submitted(
    document_build_ddb_table, mock_document_build_submit
):
    """Test submitting a build that was already submitted."""
    mock_document_build_submit["is_submitted"].return_value = True

    response = client.post("/v1/builds/test-build-id/submit")

    assert response.status_code == 400
    assert "already been submitted" in response.json()["detail"]

    # verify we didn't try to process
    mock_document_build_submit["get_pages"].assert_not_called()
    mock_document_build_submit["merge"].assert_not_called()


def test_submit_document_build_success(document_build_ddb_table, mock_document_build_submit):
    """Test successful document build submission."""
    mock_document_build_submit["get_pages"].return_value = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]

    response = client.post("/v1/builds/test-build-id/submit")

    assert response.status_code == 200
    result = response.json()
    assert "jobId" in result
    assert result["buildId"] == "test-build-id"
    assert result["jobStatus"] == "not_started"
    assert result["pageCount"] == 2

    # verify build was marked as submitted
    mock_document_build_submit["mark_submitted"].assert_called_once_with("test-build-id")


def test_upload_document_build_page_error_handling(
    document_build_ddb_table, mock_document_build_upload
):
    """Test error handling during document build page upload."""
    mock_document_build_upload["upload"].side_effect = Exception("S3 upload failed")

    files = {"file": ("page1.pdf", b"fake pdf", "application/pdf")}
    data = {"page_number": 1}
    response = client.post("/v1/builds/test-build-id/pages", files=files, data=data)

    assert response.status_code == 500
    assert "Failed to upload page" in response.json()["detail"]


def test_get_document_build_success(document_build_ddb_table, mock_document_build_submit):
    """Test getting build details."""
    pages = [
        create_page_metadata(1, category="income"),
        create_page_metadata(2),
    ]

    with (
        patch("documentai_api.utils.ddb.document_build_exists", return_value=True),
        patch("documentai_api.app.get_document_build_pages", return_value=pages),
    ):
        response = client.get("/v1/builds/test-build-id")

        assert response.status_code == 200
        result = response.json()
        assert result["buildId"] == "test-build-id"
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
            ValueError("Cannot delete - build already submitted"),
            400,
        ),  # already submitted - exception
    ],
)
def test_delete_document_build_page(
    document_build_ddb_table, monkeypatch, mock_side_effect, expected_status
):
    """Test deleting a page."""
    monkeypatch.setenv(env.DOCUMENTAI_BUILD_TABLE_NAME, "test-document-builds-table")

    with patch("documentai_api.utils.ddb.delete_document_build_page") as mock_delete:
        if isinstance(mock_side_effect, Exception):
            mock_delete.side_effect = mock_side_effect
        else:
            mock_delete.return_value = mock_side_effect

        response = client.delete("/v1/builds/test-build-id/pages/1")

        assert response.status_code == expected_status
        if expected_status == 400:
            assert "already" in response.json()["detail"]


@pytest.mark.parametrize(
    ("mock_side_effect", "expected_status"),
    [
        (True, 204),  # success
        (False, 404),  # not found
        (ValueError("Cannot delete - build already submitted"), 400),  # already submitted
    ],
)
def test_delete_document_build(
    document_build_ddb_table, monkeypatch, mock_side_effect, expected_status
):
    """Test deleting entire document build."""
    monkeypatch.setenv(env.DOCUMENTAI_BUILD_TABLE_NAME, "test-document-builds-table")

    with patch("documentai_api.utils.ddb.delete_document_build") as mock_delete:
        if isinstance(mock_side_effect, Exception):
            mock_delete.side_effect = mock_side_effect
        else:
            mock_delete.return_value = mock_side_effect

        response = client.delete("/v1/builds/test-build-id")

        assert response.status_code == expected_status
        if expected_status == 400:
            assert "already" in response.json()["detail"]
