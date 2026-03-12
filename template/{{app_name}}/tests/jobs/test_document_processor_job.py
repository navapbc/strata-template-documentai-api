"""Tests for jobs/document_processor/main.py."""

from unittest.mock import patch

import pytest

from documentai_api.config.constants import ConfigDefaults, ProcessStatus
from documentai_api.jobs.document_processor.main import (
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
    from botocore.exceptions import ClientError
    from tenacity import RetryError

    with (
        patch(
            "documentai_api.jobs.document_processor.main.invoke_bedrock_data_automation"
        ) as mock_invoke,
        patch("documentai_api.jobs.document_processor.main.classify_as_failed") as mock_classify,
    ):
        # raise ClientError so retry decorator actually retries
        mock_invoke.side_effect = ClientError(
            {"Error": {"Code": "ServiceException", "Message": "BDA invocation failed"}},
            "invoke_bedrock_data_automation",
        )

        with pytest.raises(RetryError):
            invoke_bda("test-bucket", "input/test.pdf", "test.pdf")

        mock_classify.assert_called_once()
        assert mock_classify.call_args.kwargs["object_key"] == "test.pdf"
        assert mock_classify.call_args.kwargs["error_message"] == "BDA invocation failed"


def test_main_first_time_processing():
    """Test first time processing a document."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.s3_service.head_object") as mock_head,
        patch(
            "documentai_api.jobs.document_processor.main.insert_initial_ddb_record"
        ) as mock_insert,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.side_effect = [
            ValueError("Record not found"),
            {
                DocumentMetadata.PROCESS_STATUS: ProcessStatus.NOT_STARTED.value,
            },
        ]
        mock_head.return_value = {
            "Metadata": {"original-file-name": "test.pdf"},
            "ContentLength": 1024,
            "ContentType": "application/pdf",
        }

        main("input/test.pdf", "test-bucket")

    mock_insert.assert_called_once()
    mock_invoke.assert_called_once_with("test-bucket", "input/test.pdf", "test.pdf")


def test_main_file_too_large():
    """Test file too large for BDA is marked as not implemented."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.s3_service.head_object") as mock_head,
        patch(
            "documentai_api.jobs.document_processor.main.insert_initial_ddb_record"
        ) as mock_insert,
        patch(
            "documentai_api.jobs.document_processor.main.classify_as_not_implemented"
        ) as mock_classify,
        patch("documentai_api.jobs.document_processor.main.invoke_bda") as mock_invoke,
    ):
        mock_get.side_effect = [
            ValueError("Record not found"),
            {DocumentMetadata.PROCESS_STATUS: ProcessStatus.NOT_STARTED.value},
        ]
        mock_head.return_value = {
            "Metadata": {"original-file-name": "huge.pdf"},
            "ContentLength": int(ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value) + 1,
            "ContentType": "application/pdf",
        }

        main("input/huge.pdf", "test-bucket")

    mock_insert.assert_called_once()
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
        patch("documentai_api.jobs.document_processor.main.s3_service.head_object"),
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


def test_main_reads_metadata_from_s3():
    """Test metadata is read from S3 when not provided."""
    with (
        patch("documentai_api.jobs.document_processor.main.get_ddb_record") as mock_get,
        patch("documentai_api.jobs.document_processor.main.s3_service.head_object") as mock_head,
        patch(
            "documentai_api.jobs.document_processor.main.insert_initial_ddb_record"
        ) as mock_insert,
        patch("documentai_api.jobs.document_processor.main.invoke_bda"),
    ):
        mock_get.side_effect = [
            ValueError("Record not found"),
            {DocumentMetadata.PROCESS_STATUS: ProcessStatus.NOT_STARTED.value},
        ]
        mock_head.return_value = {
            "Metadata": {
                "job-id": "test-job-id",
                "trace-id": "test-trace-id",
                "batch-id": "test-batch-id",
            },
            "ContentLength": 1024,
            "ContentType": "application/pdf",
        }

        main("input/test.pdf", "test-bucket")

        assert mock_head.call_count == 2
        assert mock_insert.call_args.kwargs["job_id"] == "test-job-id"
        assert mock_insert.call_args.kwargs["trace_id"] == "test-trace-id"
        assert mock_insert.call_args.kwargs["batch_id"] == "test-batch-id"
