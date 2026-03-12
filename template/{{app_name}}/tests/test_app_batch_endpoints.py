"""Tests for batch upload endpoints."""

import os
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from documentai_api.app import app, validate_batch_id
from documentai_api.config.constants import BatchStatus
from documentai_api.schemas.batch import Batch
from documentai_api.utils import env

client = TestClient(app)


def test_config_includes_batch_endpoints():
    """Test config endpoint includes batch endpoints."""
    response = client.get("/config")
    assert response.status_code == 200
    endpoints = response.json()["endpoints"]
    assert "batchUpload" in endpoints
    assert "batchUploadZip" in endpoints
    assert "batchUploadStatus" in endpoints


def test_batch_upload_success(pdf_file):
    """Test successful batch upload."""
    with (
        patch.dict(os.environ, {env.DOCUMENTAI_BATCH_TABLE_NAME: "test-batches-table"}),
        patch("documentai_api.app.DOCUMENTAI_MAX_BATCH_SIZE", 50),
        patch("documentai_api.app.magic.from_buffer", return_value="application/pdf"),
        patch("documentai_api.app.upload_document_for_processing"),
        patch("documentai_api.app.validate_batch_id"),
        patch("documentai_api.utils.ddb.create_batch"),
        patch("documentai_api.utils.ddb.update_batch_status"),
        patch(
            "documentai_api.utils.ddb.get_batch",
            return_value={"batchId": "test-batch", "createdAt": "2026-03-02T20:00:00Z"},
        ),
    ):
        files = [
            ("files", pdf_file("doc1.pdf")),
            ("files", pdf_file("doc2.pdf")),
        ]
        response = client.post("/v1/documents/batch", files=files)

    assert response.status_code == 200
    data = response.json()
    assert "batchId" in data
    assert data["totalFiles"] == 2
    assert len(data["jobs"]) == 2


def test_batch_upload_no_files():
    """Test batch upload with no files."""
    response = client.post("/v1/documents/batch")
    assert response.status_code == 422


def test_batch_upload_invalid_file_type():
    """Test batch upload with invalid file type."""
    with (
        patch.dict(os.environ, {env.DOCUMENTAI_BATCH_TABLE_NAME: "test-batches-table"}),
        patch("documentai_api.app.DOCUMENTAI_MAX_BATCH_SIZE", 50),
        patch("documentai_api.app.magic.from_buffer", return_value="text/plain"),
        patch("documentai_api.app.validate_batch_id"),
        patch("documentai_api.utils.ddb.create_batch"),
        patch("documentai_api.utils.ddb.update_batch_status"),
    ):
        files = [("files", ("doc.txt", b"text", "text/plain"))]
        response = client.post("/v1/documents/batch", files=files)

    assert response.status_code == 400


def test_zip_upload_success(ddb_table, zip_with_pdfs):
    """Test successful ZIP upload."""
    with (
        patch.dict(os.environ, {env.DOCUMENTAI_BATCH_TABLE_NAME: "test-batches-table"}),
        patch("documentai_api.app.DOCUMENTAI_MAX_BATCH_SIZE", 50),
        patch(
            "documentai_api.utils.zip.extract_files_from_zip", new_callable=AsyncMock
        ) as mock_extract,
        patch(
            "documentai_api.app.validate_file_type",
            new_callable=AsyncMock,
            return_value="application/pdf",
        ),
        patch("documentai_api.app.upload_document_for_processing", new_callable=AsyncMock),
        patch("documentai_api.app.validate_batch_id"),
        patch("documentai_api.utils.ddb.create_batch"),
        patch("documentai_api.utils.ddb.update_batch_status"),
        patch(
            "documentai_api.utils.ddb.get_batch",
            return_value={"batchId": "test-batch", "createdAt": "2026-03-02T20:00:00Z"},
        ),
    ):
        mock_file = MagicMock()
        mock_file.filename = "doc1.pdf"
        mock_file.file = BytesIO(b"fake pdf")
        mock_extract.return_value = [mock_file]

        zip_content = zip_with_pdfs(["doc1.pdf"])
        files = {"zip_file": ("batch.zip", zip_content, "application/zip")}
        response = client.post("/v1/documents/batch/zip", files=files)

    assert response.status_code == 200
    assert response.json()["totalFiles"] == 1


def test_zip_upload_empty(ddb_table):
    """Test ZIP upload with no valid files."""
    with (
        patch.dict(os.environ, {env.DOCUMENTAI_BATCH_TABLE_NAME: "test-batches-table"}),
        patch("documentai_api.app.DOCUMENTAI_MAX_BATCH_SIZE", 50),
        patch(
            "documentai_api.utils.zip.extract_files_from_zip", new_callable=AsyncMock
        ) as mock_extract,
        patch("documentai_api.app.validate_batch_id"),
        patch("documentai_api.utils.ddb.create_batch"),
        patch("documentai_api.utils.ddb.update_batch_status"),
    ):
        mock_extract.return_value = []
        zip_content = BytesIO(b"fake zip")
        files = {"zip_file": ("empty.zip", zip_content, "application/zip")}
        response = client.post("/v1/documents/batch/zip", files=files)

    assert response.status_code == 400


def test_get_batch_status_success():
    """Test getting batch status."""
    with (
        patch("documentai_api.utils.ddb.get_batch") as mock_get_batch,
        patch("documentai_api.utils.ddb.query_jobs_by_batch_id") as mock_query_jobs,
    ):
        mock_get_batch.return_value = {
            Batch.BATCH_ID: "test-batch-id",
            Batch.BATCH_STATUS: "processing",
            Batch.TOTAL_FILES: 2,
            Batch.CREATED_AT: "2026-02-27",
        }
        mock_query_jobs.return_value = [
            {"fileName": "doc1.pdf", "jobId": "job-1", "processStatus": "success"},
            {"fileName": "doc2.pdf", "jobId": "job-2", "processStatus": "started"},
        ]

        response = client.get("/v1/batches/test-batch-id")

    assert response.status_code == 200
    data = response.json()
    assert data["batchId"] == "test-batch-id"
    assert data["batchStatus"] == BatchStatus.PROCESSING.value
    assert data["totalJobsExpected"] == 2
    assert data["totalJobsSubmitted"] == 2
    assert data["completed"] == 1
    assert data["inProgress"] == 1


def test_get_batch_status_not_found():
    """Test getting non-existent batch."""
    with patch("documentai_api.utils.ddb.get_batch", return_value=None):
        response = client.get("/v1/batches/fake-batch")

    assert response.status_code == 404


def test_get_batch_status_lazy_completion():
    """Test that batch status updates to completed when all jobs are done."""
    with (
        patch("documentai_api.utils.ddb.get_batch") as mock_get_batch,
        patch("documentai_api.utils.ddb.query_jobs_by_batch_id") as mock_query_jobs,
        patch("documentai_api.utils.ddb.update_batch_status") as mock_update,
    ):
        mock_get_batch.return_value = {
            Batch.BATCH_ID: "test-batch",
            Batch.BATCH_STATUS: "processing",
            Batch.TOTAL_FILES: 2,
            Batch.CREATED_AT: "2026-02-27",
        }
        mock_query_jobs.return_value = [
            {"fileName": "doc1.pdf", "jobId": "job-1", "processStatus": "success"},
            {"fileName": "doc2.pdf", "jobId": "job-2", "processStatus": "failed"},
        ]

        response = client.get("/v1/batches/test-batch")

        assert response.status_code == 200
        data = response.json()
        assert data["batchStatus"] == BatchStatus.COMPLETED.value
        assert data["totalJobsExpected"] == 2
        assert data["totalJobsSubmitted"] == 2
        mock_update.assert_called_once_with("test-batch", status=BatchStatus.COMPLETED)


def test_get_batch_status_with_failed_count():
    """Test batch status includes failed count."""
    with (
        patch("documentai_api.utils.ddb.get_batch") as mock_get_batch,
        patch("documentai_api.utils.ddb.query_jobs_by_batch_id") as mock_query_jobs,
    ):
        mock_get_batch.return_value = {
            Batch.BATCH_ID: "test-batch",
            Batch.BATCH_STATUS: "processing",
            Batch.TOTAL_FILES: 3,
            Batch.CREATED_AT: "2026-02-27",
        }
        mock_query_jobs.return_value = [
            {"fileName": "doc1.pdf", "jobId": "job-1", "processStatus": "success"},
            {"fileName": "doc2.pdf", "jobId": "job-2", "processStatus": "failed"},
            {"fileName": "doc3.pdf", "jobId": "job-3", "processStatus": "started"},
        ]

        response = client.get("/v1/batches/test-batch")

        assert response.status_code == 200
        data = response.json()
        assert data["totalJobsExpected"] == 3
        assert data["totalJobsSubmitted"] == 3
        assert data["completed"] == 2
        assert data["failed"] == 1
        assert data["inProgress"] == 1


def test_get_batch_status_no_lazy_completion_when_incomplete():
    """Test batch status stays processing when jobs are still running."""
    with (
        patch("documentai_api.utils.ddb.get_batch") as mock_get_batch,
        patch("documentai_api.utils.ddb.query_jobs_by_batch_id") as mock_query_jobs,
        patch("documentai_api.utils.ddb.update_batch_status") as mock_update,
    ):
        mock_get_batch.return_value = {
            Batch.BATCH_ID: "test-batch",
            Batch.BATCH_STATUS: "processing",
            Batch.TOTAL_FILES: 2,
            Batch.CREATED_AT: "2026-02-27",
        }
        mock_query_jobs.return_value = [
            {"fileName": "doc1.pdf", "jobId": "job-1", "processStatus": "success"},
            {"fileName": "doc2.pdf", "jobId": "job-2", "processStatus": "started"},
        ]

        response = client.get("/v1/batches/test-batch")

        assert response.status_code == 200
        data = response.json()
        assert data["batchStatus"] == "processing"
        assert data["totalJobsExpected"] == 2
        assert data["totalJobsSubmitted"] == 2
        mock_update.assert_not_called()


def test_validate_batch_id_no_existing_batch():
    """Test validation passes when batch doesn't exist."""
    with patch("documentai_api.utils.ddb.get_batch") as mock_get_batch:
        mock_get_batch.return_value = None
        validate_batch_id("new-batch-id")


@pytest.mark.asyncio
async def test_upload_document_batch_exceeds_max_size(monkeypatch):
    """Test batch upload fails when exceeding DOCUMENTAI_MAX_BATCH_SIZE."""
    monkeypatch.setattr("documentai_api.app.DOCUMENTAI_MAX_BATCH_SIZE", 2)

    files = [
        ("files", ("file1.pdf", b"content1", "application/pdf")),
        ("files", ("file2.pdf", b"content2", "application/pdf")),
        ("files", ("file3.pdf", b"content3", "application/pdf")),
    ]

    response = client.post("/v1/documents/batch", files=files)

    assert response.status_code == 400
    assert "exceeds maximum of 2 files" in response.json()["detail"]


@pytest.mark.asyncio
async def test_upload_zip_batch_exceeds_max_size(monkeypatch, zip_with_pdfs):
    """Test ZIP batch upload fails when exceeding DOCUMENTAI_MAX_BATCH_SIZE."""
    monkeypatch.setattr("documentai_api.app.DOCUMENTAI_MAX_BATCH_SIZE", 2)

    mock_files = []
    for i in range(3):
        mock_file = MagicMock()
        mock_file.filename = f"file{i + 1}.pdf"
        mock_file.file = BytesIO(b"fake pdf")
        mock_files.append(mock_file)

    with (
        patch.dict(os.environ, {env.DOCUMENTAI_BATCH_TABLE_NAME: "test-table"}),
        patch(
            "documentai_api.utils.zip.extract_files_from_zip", new_callable=AsyncMock
        ) as mock_extract,
        patch("documentai_api.app.validate_batch_id"),
    ):
        mock_extract.return_value = mock_files

        zip_content = zip_with_pdfs(["file1.pdf", "file2.pdf", "file3.pdf"])
        files = [("zip_file", ("batch.zip", zip_content, "application/zip"))]
        response = client.post("/v1/documents/batch/zip", files=files)

    assert response.status_code == 400
    assert "exceeds maximum of 2 files" in response.json()["detail"]
