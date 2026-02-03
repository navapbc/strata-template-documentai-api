"""Tests for utils/s3.py."""

from unittest.mock import patch

import pytest
from documentai_api.utils import s3 as s3_util


def valid_s3_event():
    """Valid EventBridge S3 event structure."""
    return {
        "detail": {
            "bucket": {"name": "test-bucket"},
            "object": {"key": "test-file.pdf"},
        }
    }


def invalid_s3_events():
    """Invalid S3 event structures."""
    return [
        {},
        {"detail": {}},
        {"detail": {"bucket": {"name": "test"}}},
        {"detail": {"object": {"key": "test.pdf"}}},
    ]


@pytest.fixture
def mock_s3_service():
    """Mock s3_service for metadata tests."""
    with patch("documentai_api.utils.s3.s3_service") as mock:
        yield mock


def test_validate_s3_event_decorator_valid():
    """Decorator allows valid S3 event through."""

    @s3_util.validate_s3_event
    def handler(event, context):
        return {"status": "success"}

    result = handler(valid_s3_event(), None)
    assert result == {"status": "success"}


@pytest.mark.parametrize("event", invalid_s3_events())
def test_validate_s3_event_decorator_errors(event):
    """Decorator raises ValueError for invalid events."""

    @s3_util.validate_s3_event
    def handler(event, context):
        return {"status": "success"}

    with pytest.raises(ValueError, match="Missing"):
        handler(event, None)


def test_extract_s3_info_default(mock_s3_service):
    """Extract bucket and key with default parameters."""
    event = valid_s3_event()
    file_key, bucket_name = s3_util.extract_s3_info_from_event(event)

    assert file_key == "test-file.pdf"
    assert bucket_name == "test-bucket"
    mock_s3_service.head_object.assert_not_called()


@pytest.mark.parametrize("include_metadata", [False, None])
def test_extract_s3_info_include_metadata_false(mock_s3_service, include_metadata):
    """Extract without metadata when explicitly False or None."""
    event = valid_s3_event()
    file_key, bucket_name = s3_util.extract_s3_info_from_event(
        event, include_metadata=include_metadata
    )

    assert file_key == "test-file.pdf"
    assert bucket_name == "test-bucket"
    mock_s3_service.head_object.assert_not_called()


@pytest.mark.parametrize(
    ("head_object_response","expected_metadata"),
    [
        ({"Metadata": {"job-id": "123", "trace-id": "abc"}}, {"job-id": "123", "trace-id": "abc"}),
        ({}, {}),
    ],
)
def test_extract_s3_info_include_metadata_true(
    mock_s3_service, head_object_response, expected_metadata
):
    """Extract bucket, key, and metadata from event."""
    mock_s3_service.head_object.return_value = head_object_response

    event = valid_s3_event()
    file_key, bucket_name, metadata = s3_util.extract_s3_info_from_event(
        event, include_metadata=True
    )

    assert file_key == "test-file.pdf"
    assert bucket_name == "test-bucket"
    assert metadata == expected_metadata
    mock_s3_service.head_object.assert_called_once_with("test-bucket", "test-file.pdf")


@pytest.mark.parametrize("event", invalid_s3_events())
def test_extract_s3_info_invalid_events(event):
    """Raise ValueError for invalid event structures."""
    with pytest.raises(ValueError, match="Invalid EventBridge event structure"):
        s3_util.extract_s3_info_from_event(event)
