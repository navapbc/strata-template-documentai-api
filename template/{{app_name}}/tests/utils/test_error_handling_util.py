"""Tests for utils/error_handling.py."""

from unittest.mock import MagicMock, patch

import pytest
from documentai_api.utils.error_handling import handle_lambda_errors


@pytest.fixture
def mock_context():
    """Mock Lambda context."""
    return MagicMock()


@pytest.fixture
def s3_event():
    """Create S3 event with given key."""

    def _event(key="test-file.pdf"):
        return {"Records": [{"s3": {"bucket": {"name": "test-bucket"}, "object": {"key": key}}}]}

    return _event


def create_failing_handler(exception):
    """Helper to create a handler that raises the given exception."""

    @handle_lambda_errors
    def handler(event, context):
        raise exception

    return handler


def test_handle_lambda_errors_success(s3_event, mock_context):
    """Test decorator allows successful handler execution."""

    @handle_lambda_errors
    def handler(event, context):
        return {"statusCode": 200, "body": "success"}

    result = handler(s3_event(), mock_context)

    assert result["statusCode"] == 200
    assert result["body"] == "success"


@pytest.mark.parametrize(
    ("s3_key","expected_filename"),
    [
        ("test-file.pdf", "test-file.pdf"),
        (
            "processed/w2-abc123.pdf/job-id/0/custom_output/0/result.json",
            "w2-abc123.pdf",
        ),
        (
            "processed/1099-form.pdf/job-456/0/custom_output/0/result.json",
            "1099-form.pdf",
        ),
    ],
)
def test_handle_lambda_errors_on_failure(s3_event, s3_key, expected_filename, mock_context):
    """Test decorator catches exceptions, logs errors, returns 500, and updates DDB."""
    error_message = "Test error"
    handler = create_failing_handler(ValueError(error_message))

    with (patch("documentai_api.utils.error_handling.logger") as mock_logger,
         patch("documentai_api.utils.ddb.classify_as_failed") as mock_classify_as_failed,
             patch(
                "documentai_api.utils.s3.extract_s3_info_from_event",
                return_value=(s3_key, "test-bucket"),
            )):
                result = handler(s3_event(s3_key), mock_context)

    # verify error response
    assert result["statusCode"] == 500
    assert error_message in result["body"]

    # verify ddb update with correct filename
    assert mock_classify_as_failed.call_args.kwargs["object_key"] == expected_filename

    # verify error was logged
    assert any(error_message in str(call) for call in mock_logger.error.call_args_list)


def test_handle_lambda_errors_when_ddb_update_fails(s3_event, mock_context):
    """Test decorator handles DDB update failures gracefully."""
    error_message = "Handler failed"
    handler = create_failing_handler(Exception(error_message))

    with (patch("documentai_api.utils.ddb.classify_as_failed", side_effect=Exception("DDB error")),
        patch(
            "documentai_api.utils.s3.extract_s3_info_from_event",
            return_value=("test-file.pdf", "test-bucket"),
        )):
            result = handler(s3_event(), mock_context)

    assert result["statusCode"] == 500
