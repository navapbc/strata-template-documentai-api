"""Tests for scripts/bda_output_processor.py"""

from unittest.mock import patch

import pytest
from scripts.bda_output_processor import extract_uploaded_filename, main


@pytest.mark.parametrize(
    "object_key,expected_filename",
    [
        ("processed/test-file.pdf/output.json", "test-file.pdf"),
        ("processed/document.png/output.json", "document.png"),
        ("processed/file_truncated.pdf/output.json", "file.pdf"),
        ("processed/report_truncated.png/output.json", "report.png"),
    ],
)
def test_extract_uploaded_filename_success(object_key, expected_filename):
    """Test extracting filename from valid BDA output paths."""
    result = extract_uploaded_filename(object_key)
    assert result == expected_filename


@pytest.mark.parametrize(
    "invalid_key",
    [
        "invalid/path/output.json",
        "output.json",
        "processed",
        "other-prefix/file.pdf/output.json",
    ],
)
def test_extract_uploaded_filename_invalid_path(invalid_key):
    """Test error handling for invalid BDA output paths."""
    with pytest.raises(ValueError, match="Invalid BDA output path"):
        extract_uploaded_filename(invalid_key)


def test_main_success():
    """Test successful BDA output processing."""
    with patch("scripts.bda_output_processor.get_api_response_data") as mock_get_data:
        mock_get_data.return_value = {"status": "success", "data": {"field1": "value1"}}
        result = main("test-bucket", "processed/test-file.pdf/output.json")

    assert result == {"status": "success", "data": {"field1": "value1"}}
    mock_get_data.assert_called_once_with(
        "test-file.pdf", "test-bucket", "processed/test-file.pdf/output.json"
    )


def test_main_with_truncated_filename():
    """Test processing BDA output with truncated filename."""
    with patch("scripts.bda_output_processor.get_api_response_data") as mock_get_data:
        mock_get_data.return_value = {"status": "success"}
        main("test-bucket", "processed/long_truncated.pdf/output.json")

    mock_get_data.assert_called_once_with(
        "long.pdf", "test-bucket", "processed/long_truncated.pdf/output.json"
    )


def test_main_invalid_object_key():
    """Test error handling for invalid object key."""
    with pytest.raises(ValueError, match="Invalid BDA output path"):
        main("test-bucket", "invalid/path.json")
