"""Tests for utils/s3.py."""

import pytest

from documentai_api.utils import s3 as s3_util


@pytest.mark.parametrize(
    ("s3_uri", "expected_bucket", "expected_key"),
    [
        ("s3://bucket/key", "bucket", "key"),
        ("s3://my-bucket/path/to/file.json", "my-bucket", "path/to/file.json"),
        ("s3://bucket/prefix/input/file.pdf", "bucket", "prefix/input/file.pdf"),
        ("s3://bucket", "bucket", ""),  # No key
    ],
)
def test_parse_s3_uri(s3_uri, expected_bucket, expected_key):
    """Parse S3 URIs into bucket and key."""
    bucket, key = s3_util.parse_s3_uri(s3_uri)
    assert bucket == expected_bucket
    assert key == expected_key


@pytest.mark.parametrize(
    ("s3_location", "expected_prefix"),
    [
        ("s3://bucket/input", "input"),
        ("s3://bucket/processed", "processed"),
        ("s3://bucket/path/to/files", "path/to/files"),
        ("s3://bucket", ""),  # No prefix
        ("", ""),  # Empty string
    ],
)
def test_get_s3_prefix_from_location(s3_location, expected_prefix):
    """Extract prefix from S3 location."""
    prefix = s3_util.get_s3_prefix_from_location(s3_location)
    assert prefix == expected_prefix
