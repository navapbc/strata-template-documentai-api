from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from documentai_api.app import app, generate_unique_filename

client = TestClient(app)


def test_create_upload_url_success():
    """Test successful presigned URL generation."""
    with (
        patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input"),
        patch("documentai_api.app.s3_service.generate_presigned_url") as mock_generate_url,
    ):
        mock_generate_url.return_value = "https://s3.amazonaws.com/presigned-url"

        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
        }
        response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["uploadUrl"] == "https://s3.amazonaws.com/presigned-url"
    assert data["method"] == "PUT"
    assert "jobId" in data
    assert "Content-Type" in data["headers"]
    assert data["headers"]["Content-Type"] == "application/pdf"
    assert "x-amz-meta-job-id" in data["headers"]


def test_create_upload_url_with_trace_id():
    """Test presigned URL generation with trace_id."""
    with (
        patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input"),
        patch("documentai_api.app.s3_service.generate_presigned_url") as mock_generate_url,
    ):
        mock_generate_url.return_value = "https://s3.amazonaws.com/presigned-url"

        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
            "trace_id": "trace-123",
        }
        response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert "x-amz-meta-trace-id" in data["headers"]
    assert data["headers"]["x-amz-meta-trace-id"] == "trace-123"


def test_create_upload_url_with_category():
    """Test presigned URL generation with user_provided_document_category."""
    with (
        patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input"),
        patch("documentai_api.app.s3_service.generate_presigned_url") as mock_generate_url,
    ):
        mock_generate_url.return_value = "https://s3.amazonaws.com/presigned-url"

        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
            "user_provided_document_category": "income",
        }
        response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert "x-amz-meta-user-provided-document-category" in data["headers"]
    assert data["headers"]["x-amz-meta-user-provided-document-category"] == "income"


def test_create_upload_url_with_all_metadata():
    """Test presigned URL generation with all optional metadata."""
    with (
        patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input"),
        patch("documentai_api.app.s3_service.generate_presigned_url") as mock_generate_url,
    ):
        mock_generate_url.return_value = "https://s3.amazonaws.com/presigned-url"

        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
            "trace_id": "trace-123",
            "user_provided_document_category": "income",
        }
        response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["headers"]["x-amz-meta-trace-id"] == "trace-123"
    assert data["headers"]["x-amz-meta-user-provided-document-category"] == "income"
    assert "x-amz-meta-job-id" in data["headers"]


def test_create_upload_url_unsupported_content_type():
    """Test presigned URL generation with unsupported content type."""
    payload = {
        "filename": "test.zip",
        "content_type": "application/zip",
    }
    response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 400
    assert "Unsupported content type" in response.json()["detail"]


def test_create_upload_url_no_input_location():
    """Test presigned URL generation when DOCUMENTAI_INPUT_LOCATION not set."""
    with patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", None):
        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
        }
        response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 500
    assert "Upload location not configured" in response.json()["detail"]


def test_create_upload_url_s3_error():
    """Test presigned URL generation when S3 service fails."""
    with (
        patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input"),
        patch("documentai_api.app.s3_service.generate_presigned_url") as mock_generate_url,
    ):
        mock_generate_url.side_effect = Exception("S3 error")

        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
        }
        response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 500
    assert "Failed to generate upload URL" in response.json()["detail"]


def test_create_upload_url_generates_unique_filename():
    """Test that presigned URL uses unique filename with UUID."""
    with (
        patch("documentai_api.app.DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input"),
        patch("documentai_api.app.s3_service.generate_presigned_url") as mock_generate_url,
    ):
        mock_generate_url.return_value = "https://s3.amazonaws.com/presigned-url"

        payload = {
            "filename": "test.pdf",
            "content_type": "application/pdf",
        }
        client.post("/v1/documents/presigned-url", json=payload)

        call_kwargs = mock_generate_url.call_args[1]
        assert call_kwargs["bucket"] == "test-bucket"
        assert call_kwargs["key"].startswith("input/test-")
        assert call_kwargs["key"].endswith(".pdf")
        assert call_kwargs["content_type"] == "application/pdf"
        assert "metadata" in call_kwargs
        assert "job-id" in call_kwargs["metadata"]
        assert "trace-id" in call_kwargs["metadata"]


def test_create_upload_url_missing_filename():
    """Test presigned URL generation with missing filename."""
    payload = {
        "content_type": "application/pdf",
    }
    response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 422


def test_create_upload_url_missing_content_type():
    """Test presigned URL generation with missing content_type."""
    payload = {
        "filename": "test.pdf",
    }
    response = client.post("/v1/documents/presigned-url", json=payload)

    assert response.status_code == 422


def test_generate_unique_filename():
    """Test generate_unique_filename function."""
    result = generate_unique_filename("test.pdf")

    assert result.startswith("test-")
    assert result.endswith(".pdf")
    assert len(result) > len("test-.pdf")


def test_generate_unique_filename_empty():
    """Test generate_unique_filename with empty string."""
    with pytest.raises(ValueError, match="Invalid filename"):
        generate_unique_filename("")
