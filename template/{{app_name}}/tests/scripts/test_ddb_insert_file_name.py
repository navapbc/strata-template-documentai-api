"""Tests for scripts/ddb_insert_file_name.py"""

from unittest.mock import MagicMock, patch

import pytest
from config.constants import ConfigDefaults
from scripts.ddb_insert_file_name import (
    convert_s3_object_to_grayscale,
    convert_to_grayscale,
    is_file_too_large_for_bda,
    main,
)

@pytest.fixture
def mock_grayscale_dependencies():
    with patch("cv2.imdecode") as mock_cv2_imdecode, \
         patch("cv2.cvtColor") as mock_c2_cvtColor, \
         patch("PIL.Image.fromarray") as mock_pil_fromarray:
        yield mock_cv2_imdecode, mock_c2_cvtColor, mock_pil_fromarray

@pytest.mark.parametrize(
    "content_type,file_size,expected",
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


def test_convert_s3_object_to_grayscale_success():
    """Test successful S3 object grayscale conversion."""
    with patch("scripts.ddb_insert_file_name.s3_service.get_object") as mock_s3_get:
        with patch("scripts.ddb_insert_file_name.s3_service.put_object") as mock_s3_put:
            with patch("scripts.ddb_insert_file_name.convert_to_grayscale") as mock_convert_to_grayscale:
                mock_s3_get.return_value = {
                    "Body": MagicMock(read=lambda: b"image data"),
                    "ContentType": "image/jpeg",
                }
                mock_convert_to_grayscale.return_value = (b"grayscale data", "image/jpeg")
                convert_s3_object_to_grayscale("test-bucket", "test.jpg")

    mock_s3_get.assert_called_once_with("test-bucket", "test.jpg")
    mock_convert_to_grayscale.assert_called_once_with("test.jpg", b"image data", "image/jpeg")
    mock_s3_put.assert_called_once_with("test-bucket", "test.jpg", b"grayscale data", "image/jpeg")


def test_main_first_time_processing_not_started():
    """Test first time processing file that goes to not_started status."""
    with patch("scripts.ddb_insert_file_name.get_ddb_record") as mock_ddb_get:
        with patch("scripts.ddb_insert_file_name.insert_initial_ddb_record"):
            mock_ddb_get.side_effect = [
                ValueError("Record not found"),
                {"processStatus": "not_started"},
            ]

            result = main("test-bucket", "test.pdf")

    assert result == {"statusCode": 200}


def test_main_first_time_processing_pending_grayscale():
    """Test first time processing file that needs grayscale conversion."""
    with patch("scripts.ddb_insert_file_name.get_ddb_record") as mock_ddb_get:
        with patch("scripts.ddb_insert_file_name.insert_initial_ddb_record"):
            with patch("scripts.ddb_insert_file_name.convert_s3_object_to_grayscale") as mock_convert:
                mock_ddb_get.side_effect = [
                    ValueError("Record not found"),
                    {"processStatus": "pending_grayscale_conversion"},
                ]

                result = main("test-bucket", "test.jpg")

    mock_convert.assert_called_once_with("test-bucket", "test.jpg")
    assert result == {"statusCode": 200}


def test_main_second_event_grayscale_file_not_too_large():
    """Test processing grayscale file that is not too large."""
    with patch("scripts.ddb_insert_file_name.get_ddb_record") as mock_ddb_get:
        with patch("scripts.ddb_insert_file_name.s3_service.head_object") as mock_head:
            with patch("scripts.ddb_insert_file_name.set_bda_processing_status_not_started") as mock_set_status:
                mock_ddb_get.return_value = {
                    "processStatus": "pending_grayscale_conversion",
                    "userProvidedDocumentCategory": "income",
                }
                mock_head.return_value = {
                    "ContentLength": 1_000_000,
                    "ContentType": "image/jpeg",
                }

                result = main("test-bucket", "test.jpg")

    mock_set_status.assert_called_once_with("test.jpg")
    assert result == {"statusCode": 200}


def test_main_second_event_grayscale_file_too_large():
    """Test processing grayscale file that exceeds BDA limits."""
    with patch("scripts.ddb_insert_file_name.get_ddb_record") as mock_ddb_get:
        with patch("scripts.ddb_insert_file_name.s3_service.head_object") as mock_head:
            with patch("scripts.ddb_insert_file_name.classify_as_not_implemented") as mock_classify_as_not_implemented:
                mock_ddb_get.return_value = {
                    "processStatus": "pending_grayscale_conversion",
                    "userProvidedDocumentCategory": "income",
                }
                mock_head.return_value = {
                    "ContentLength": 10_000_000,
                    "ContentType": "image/jpeg",
                }

                result = main("test-bucket", "test.jpg")

    mock_classify_as_not_implemented.assert_called_once()
    assert result == {"statusCode": 200}


def test_main_already_processed():
    """Test that already processed files are skipped."""
    with patch("scripts.ddb_insert_file_name.get_ddb_record") as mock_ddb_get:
        mock_ddb_get.return_value = {"processStatus": "success"}

        result = main("test-bucket", "test.pdf")

    assert result == {"statusCode": 200}


def test_main_with_metadata():
    """Test processing with optional metadata parameters."""
    with patch("scripts.ddb_insert_file_name.get_ddb_record") as mock_ddb_get:
        with patch("scripts.ddb_insert_file_name.insert_initial_ddb_record") as mock_insert_ddb:
            mock_ddb_get.side_effect = [
                ValueError("Record not found"),
                {"processStatus": "not_started"},
            ]

            result = main(
                "test-bucket",
                "test.pdf",
                user_provided_document_category="income",
                job_id="job-123",
                trace_id="trace-456",
            )

    mock_insert_ddb.assert_called_once_with(
        source_bucket_name="test-bucket",
        source_object_key="test.pdf",
        user_provided_document_category="income",
        job_id="job-123",
        trace_id="trace-456",
    )
    assert result == {"statusCode": 200}



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

    mock_cv2_imdecode, mock_cv2_cvtColor, mock_pil_fromarray = mock_grayscale_dependencies

    # mock image processing
    mock_img = MagicMock()
    mock_cv2_imdecode.return_value = mock_img
    mock_cv2_cvtColor.return_value = MagicMock()
    
    mock_pil = MagicMock()
    mock_pil_fromarray.return_value = mock_pil
    mock_pil.save = mock_save
    
    result_bytes, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")
    
    assert result_type == "image/jpeg"
    assert len(result_bytes) > 0


def test_convert_to_grayscale_large_image_converts_to_pdf(mock_grayscale_dependencies):
    """Test large image converts to PDF."""

    mock_cv2_imdecode, mock_cv2_cvtColor, mock_pil_fromarray = mock_grayscale_dependencies


    mock_cv2_imdecode.return_value = MagicMock()
    mock_cv2_cvtColor.return_value = MagicMock()
    
    mock_pil = MagicMock()
    mock_pil_fromarray.return_value = mock_pil
    
    def save_side_effect(buf, format, quality=None):
        if format == "JPEG":
            # write more than the actual limit
            buf.write(b"x" * (int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1))
        else:
            buf.write(b"pdf data")
    
    mock_pil.save = save_side_effect
    
    _, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")
    
    assert result_type == "application/pdf"


def test_convert_s3_object_to_grayscale_error():
    """Test S3 grayscale conversion handles errors gracefully."""
    with patch("scripts.ddb_insert_file_name.s3_service.get_object") as mock_s3_get:
        mock_s3_get.side_effect = Exception("S3 error")
        
        # should not raise, just log error
        convert_s3_object_to_grayscale("test-bucket", "test.jpg")
