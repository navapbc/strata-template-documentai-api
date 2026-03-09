"""Tests for jobs/document_processor/main.py."""

from unittest.mock import MagicMock, patch

import pytest

from documentai_api.config.constants import ConfigDefaults, ProcessStatus
from documentai_api.jobs.document_processor.main import (
    convert_s3_object_to_grayscale,
    convert_to_grayscale,
    invoke_bda,
    is_file_too_large_for_bda,
    main,
)
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.utils import env


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Mock environment variables for all tests."""
    monkeypatch.setenv(env.DOCUMENTAI_INPUT_LOCATION, "s3://test-bucket/input")


@pytest.mark.parametrize(
    ("content_type", "file_size", "expected"),
    [
        ("image/jpeg", ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value, False),
        ("image/jpeg", int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1, True),
        ("image/png", ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value, False),
        ("image/png", int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1, True),
        ("application/pdf", ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value, False),
        ("application/pdf", int(ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value) + 1, True),
        ("image/tiff", ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value, False),
        ("image/tiff", int(ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value) + 1, True),
        ("unknown/type", int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1, True),
    ],
)
def test_is_file_too_large_for_bda(content_type, file_size, expected):
    """Test file size validation for BDA limits."""
    result = is_file_too_large_for_bda(content_type, file_size)
    assert result == expected


def test_convert_to_grayscale_non_image():
    """Test that non-image files are returned unchanged."""
    file_bytes = b"pdf content"
    result_bytes, result_type = convert_to_grayscale("test.pdf", file_bytes, "application/pdf")

    assert result_bytes == file_bytes
    assert result_type == "application/pdf"


def test_convert_to_grayscale_invalid_image():
    """Test grayscale conversion with invalid image data."""
    file_bytes = b"not an image"
    result_bytes, result_type = convert_to_grayscale("test.jpg", file_bytes, "image/jpeg")

    assert result_bytes == file_bytes
    assert result_type == "image/jpeg"


def test_convert_to_grayscale_small_image(mock_grayscale_dependencies):
    """Test grayscale conversion with small valid image."""

    def mock_save(buf, format, quality=None):
        buf.write(b"small jpeg")

    mock_cv2_imdecode, mock_cv2_cvtcolor, mock_pil_fromarray = mock_grayscale_dependencies

    mock_img = MagicMock()
    mock_cv2_imdecode.return_value = mock_img
    mock_cv2_cvtcolor.return_value = MagicMock()

    mock_pil = MagicMock()
    mock_pil_fromarray.return_value = mock_pil
    mock_pil.save = mock_save

    result_bytes, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")

    assert result_type == "image/jpeg"
    assert len(result_bytes) > 0


def test_convert_to_grayscale_large_image_converts_to_pdf(mock_grayscale_dependencies):
    """Test large image converts to PDF."""
    mock_cv2_imdecode, mock_cv2_cvtcolor, mock_pil_fromarray = mock_grayscale_dependencies

    mock_cv2_imdecode.return_value = MagicMock()
    mock_cv2_cvtcolor.return_value = MagicMock()

    mock_pil = MagicMock()
    mock_pil_fromarray.return_value = mock_pil

    def save_side_effect(buf, format, quality=None):
        if format == "JPEG":
            buf.write(b"x" * (int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1))
        else:
            buf.write(b"pdf data")

    mock_pil.save = save_side_effect

    _, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")

    assert result_type == "application/pdf"


def test_convert_s3_object_to_grayscale_success():
    """Test successful S3 object grayscale conversion."""
    with (
        patch("documentai_api.jobs.document_processor.main.s3_service.get_object") as mock_s3_get,
        patch("documentai_api.jobs.document_processor.main.s3_service.put_object") as mock_s3_put,
        patch("documentai_api.jobs.document_processor.main.convert_to_grayscale") as mock_convert,
    ):
        mock_s3_get.return_value = {
            "Body": MagicMock(read=lambda: b"image data"),
            "ContentType": "image/jpeg",
        }
        mock_convert.return_value = (b"grayscale data", "image/jpeg")

        result = convert_s3_object_to_grayscale("test-bucket", "test.jpg")

    assert result is True
    mock_s3_get.assert_called_once_with("test-bucket", "test.jpg")
    mock_convert.assert_called_once_with("test.jpg", b"image data", "image/jpeg")
    mock_s3_put.assert_called_once_with("test-bucket", "test.jpg", b"grayscale data", "image/jpeg")


def test_convert_s3_object_to_grayscale_file_too_large():
    """Test S3 conversion returns False when file too large."""
    with (
        patch("documentai_api.jobs.document_processor.main.s3_service.get_object") as mock_s3_get,
        patch("documentai_api.jobs.document_processor.main.convert_to_grayscale") as mock_convert,
    ):
        mock_s3_get.return_value = {
            "Body": MagicMock(read=lambda: b"image data"),
            "ContentType": "image/jpeg",
        }
        large_bytes = b"x" * (int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1)
        mock_convert.return_value = (large_bytes, "image/jpeg")

        result = convert_s3_object_to_grayscale("test-bucket", "test.jpg")

    assert result is False


def test_convert_s3_object_to_grayscale_error():
    """Test S3 grayscale conversion handles errors gracefully."""
    with patch("documentai_api.jobs.document_processor.main.s3_service.get_object") as mock_s3_get:
        mock_s3_get.side_effect = Exception("S3 error")

        result = convert_s3_object_to_grayscale("test-bucket", "test.jpg")

    assert result is False


def test_invoke_bda_success():
    """Test successful BDA invocation."""
    with (
        patch(
            "documentai_api.jobs.document_processor.main.invoke_bedrock_data_automation"
        ) as mock_invoke,
        patch(
            "documentai_api.jobs.document_processor.main.set_bda_processing_status_started"
        ) as mock_set_status,
    ):
        mock_invoke.return_value = "arn:aws:bedrock:us-east-1:123456789012:job/abc123"

        result = invoke_bda("test-bucket", "input/test.pdf", "test.pdf")

    assert result["invocationArn"] == "arn:aws:bedrock:us-east-1:123456789012:job/abc123"
    mock_set_status.assert_called_once_with(
        object_key="test.pdf",
        bda_invocation_arn="arn:aws:bedrock:us-east-1:123456789012:job/abc123",
    )


def test_invoke_bda_failure():
    """Test BDA invocation failure updates DDB and raises exception."""
    with (
        patch(
            "documentai_api.jobs.document_processor.main.invoke_bedrock_data_automation"
        ) as mock_invoke,
        patch("documentai_api.jobs.document_processor.main.classify_as_failed") as mock_classify,
    ):
        mock_invoke.side_effect = Exception("BDA service error")

        with pytest.raises(Exception, match="BDA service error"):
            invoke_bda("test-bucket", "input/test.pdf", "test.pdf")

        mock_classify.assert_called_once()
        assert mock_classify.call_args.kwargs["object_key"] == "test.pdf"
        assert "BDA invocation failed" in mock_classify.call_args.kwargs["error_message"]


def test_main_first_time_pdf():
    """Test first time processing PDF (no grayscale needed)."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch(
            "documentai_api.jobs.document_processor.main.insert_initial_ddb_record"
        ) as mock_insert,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.side_effect = [
            ValueError("Record not found"),
            {DocumentMetadata.PROCESS_STATUS: ProcessStatus.NOT_STARTED.value},
        ]

        main("input/test.pdf", "test-bucket")

    mock_insert.assert_called_once()
    mock_invoke.assert_called_once_with("test-bucket", "input/test.pdf", "test.pdf")


def test_main_first_time_image():
    """Test first time processing image (needs grayscale)."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch(
            "documentai_api.jobs.document_processor.main.insert_initial_ddb_record"
        ) as mock_insert,
        patch(
            "documentai_api.jobs.document_processor.main.convert_s3_object_to_grayscale"
        ) as mock_convert,
        patch(
            "documentai_api.jobs.document_processor.main.set_bda_processing_status_not_started"
        ) as mock_set_status,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.side_effect = [
            ValueError("Record not found"),
            {DocumentMetadata.PROCESS_STATUS: ProcessStatus.PENDING_GRAYSCALE_CONVERSION},
        ]
        mock_convert.return_value = True

        main("input/test.jpg", "test-bucket")

    mock_insert.assert_called_once()
    mock_convert.assert_called_once_with("test-bucket", "input/test.jpg")
    mock_set_status.assert_called_once_with("test.jpg")
    mock_invoke.assert_called_once_with("test-bucket", "input/test.jpg", "test.jpg")


def test_main_grayscale_conversion_fails():
    """Test grayscale conversion failure marks as not implemented."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.insert_initial_ddb_record"),
        patch(
            "documentai_api.jobs.document_processor.main.convert_s3_object_to_grayscale"
        ) as mock_convert,
        patch(
            "documentai_api.jobs.document_processor.main.classify_as_not_implemented"
        ) as mock_classify,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.side_effect = [
            ValueError("Record not found"),
            {DocumentMetadata.PROCESS_STATUS: ProcessStatus.PENDING_GRAYSCALE_CONVERSION},
        ]
        mock_convert.return_value = False

        main("input/test.jpg", "test-bucket")

    mock_classify.assert_called_once()
    mock_invoke.assert_not_called()


def test_main_already_processed():
    """Test that already processed files are skipped."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.return_value = {DocumentMetadata.PROCESS_STATUS: "success"}

        main("input/test.pdf", "test-bucket")

    mock_invoke.assert_not_called()


def test_main_uses_env_bucket_when_not_provided():
    """Test bucket name defaults to environment variable."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.return_value = {DocumentMetadata.PROCESS_STATUS: ProcessStatus.NOT_STARTED.value}

        main("input/test.pdf")

    mock_invoke.assert_called_once_with("test-bucket", "input/test.pdf", "test.pdf")


def test_main_idempotent_on_duplicate_events():
    """Test job is idempotent when receiving duplicate S3 events."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.return_value = {DocumentMetadata.PROCESS_STATUS: "processing"}

        main("input/test.pdf", "test-bucket")

    mock_invoke.assert_not_called()
