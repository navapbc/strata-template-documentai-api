"""Tests for jobs/bda_result_processor/cli.py."""

from unittest.mock import patch

import pytest

from documentai_api.jobs.bda_result_processor.cli import extract_uploaded_filename, main


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Mock environment variables for all tests."""
    monkeypatch.setenv("DOCUMENTAI_OUTPUT_LOCATION", "s3://test-bucket/processed")
    monkeypatch.setenv("DOCUMENTAI_INPUT_LOCATION", "s3://test-bucket/input")


@pytest.mark.parametrize(
    ("object_key", "expected_filename"),
    [
        ("processed/input/test-file.pdf/job_metadata.json", "test-file.pdf"),
        ("processed/input/document.png/job_metadata.json", "document.png"),
        ("processed/input/file_truncated.pdf/job_metadata.json", "file.pdf"),
        ("processed/input/report_truncated.png/job_metadata.json", "report.png"),
    ],
)
def test_extract_uploaded_filename_success(object_key, expected_filename):
    """Test extracting filename from valid BDA output paths."""
    result = extract_uploaded_filename(object_key)
    assert result == expected_filename


def test_main_success():
    """Test successful BDA output processing."""
    with patch("documentai_api.jobs.bda_result_processor.cli.process_bda_output") as mock_get_data:
        mock_get_data.return_value = {"status": "success", "data": {"field1": "value1"}}
        result = main("test-bucket", "processed/input/test-file.pdf/job_metadata.json")

    assert result == {"status": "success", "data": {"field1": "value1"}}
    mock_get_data.assert_called_once_with(
        "test-file.pdf", "test-bucket", "processed/input/test-file.pdf/job_metadata.json"
    )


def test_main_with_truncated_filename():
    """Test processing BDA output with truncated filename."""
    with patch("documentai_api.jobs.bda_result_processor.cli.process_bda_output") as mock_get_data:
        mock_get_data.return_value = {"status": "success"}
        main("test-bucket", "processed/input/long_truncated.pdf/job_metadata.json")

    mock_get_data.assert_called_once_with(
        "long.pdf", "test-bucket", "processed/input/long_truncated.pdf/job_metadata.json"
    )


def test_main_skips_non_metadata_files():
    """Test that non-metadata files are skipped."""
    with patch("documentai_api.jobs.bda_result_processor.cli.process_bda_output") as mock_process:
        result = main("test-bucket", "processed/input/test-file.pdf/.s3_access_check")

    assert result == {}
    mock_process.assert_not_called()
