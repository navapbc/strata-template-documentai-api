"""Tests for S3 Service methods."""

import pytest
from moto import mock_aws

from documentai_api.services import s3 as s3_service


@pytest.fixture
def s3_bucket(aws_credentials):
    """Create a test S3 bucket."""
    import boto3

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        yield s3


@mock_aws
def test_upload_file_args(s3_bucket):
    """Upload file to S3."""
    from io import BytesIO

    file_obj = BytesIO(b"test data")
    s3_service.upload_file(
        "test-bucket", "test-key", file_obj, content_type="text/plain", metadata={"foo": "bar"}
    )

    obj = s3_bucket.head_object(Bucket="test-bucket", Key="test-key")
    assert obj["ContentType"] == "text/plain"
    assert obj["Metadata"] == {"foo": "bar"}


@mock_aws
def test_upload_file_no_args(s3_bucket):
    """Upload file without content type or metadata."""
    from io import BytesIO

    file_obj = BytesIO(b"test data")
    s3_service.upload_file("test-bucket", "test-key", file_obj)

    obj = s3_bucket.get_object(Bucket="test-bucket", Key="test-key")
    assert obj["Body"].read() == b"test data"


@mock_aws
def test_get_object(s3_bucket):
    """Get object from S3."""
    s3_bucket.put_object(Bucket="test-bucket", Key="test-key", Body=b"data")

    result = s3_service.get_object("test-bucket", "test-key")
    assert result["Body"].read() == b"data"


@mock_aws
def test_head_object(s3_bucket):
    """Get object metadata from S3."""
    s3_bucket.put_object(
        Bucket="test-bucket", Key="test-key", Body=b"data", ContentType="application/pdf"
    )

    result = s3_service.head_object("test-bucket", "test-key")
    assert result["ContentType"] == "application/pdf"
    assert result["ContentLength"] == 4


@mock_aws
def test_put_object(s3_bucket):
    """Put object to S3 with content type."""
    s3_service.put_object("test-bucket", "test-key", b"data", content_type="text/plain")

    obj = s3_bucket.get_object(Bucket="test-bucket", Key="test-key")
    assert obj["Body"].read() == b"data"
    assert obj["ContentType"] == "text/plain"


@mock_aws
def test_put_object_no_content_type(s3_bucket):
    """Put object to S3 without content type."""
    s3_service.put_object("test-bucket", "test-key", b"data")

    obj = s3_bucket.get_object(Bucket="test-bucket", Key="test-key")
    assert obj["Body"].read() == b"data"


@mock_aws
def test_get_content_type(s3_bucket):
    """Get file content type."""
    s3_bucket.put_object(
        Bucket="test-bucket", Key="test-key", Body=b"data", ContentType="application/pdf"
    )

    result = s3_service.get_content_type("test-bucket", "test-key")
    assert result == "application/pdf"


@mock_aws
def test_get_content_type_default(s3_bucket):
    """Get file content type with default fallback."""
    s3_bucket.put_object(Bucket="test-bucket", Key="test-key", Body=b"data")

    result = s3_service.get_content_type("test-bucket", "test-key")
    assert result == "binary/octet-stream"


@mock_aws
def test_get_file_size_bytes(s3_bucket):
    """Get file size in bytes."""
    s3_bucket.put_object(Bucket="test-bucket", Key="test-key", Body=b"12345")

    result = s3_service.get_file_size_bytes("test-bucket", "test-key")
    assert result == 5


@mock_aws
def test_get_file_bytes(s3_bucket):
    """Get file content as bytes."""
    s3_bucket.put_object(Bucket="test-bucket", Key="test-key", Body=b"file content")

    result = s3_service.get_file_bytes("test-bucket", "test-key")
    assert result == b"file content"


@mock_aws
def test_is_password_protected_true(s3_bucket):
    """Check if PDF is password protected - encrypted."""
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="test-key",
        Body=b"/Encrypt some pdf data",
        ContentType="application/pdf",
    )

    result = s3_service.is_password_protected("test-bucket", "test-key")
    assert result is True


@mock_aws
def test_is_password_protected_false(s3_bucket):
    """Check if PDF is password protected - not encrypted."""
    s3_bucket.put_object(
        Bucket="test-bucket", Key="test-key", Body=b"normal pdf data", ContentType="application/pdf"
    )

    result = s3_service.is_password_protected("test-bucket", "test-key")
    assert result is False


@mock_aws
def test_is_password_protected_not_pdf(s3_bucket):
    """Check if non-PDF is password protected - returns False."""
    s3_bucket.put_object(
        Bucket="test-bucket", Key="test-key", Body=b"image data", ContentType="image/jpeg"
    )

    result = s3_service.is_password_protected("test-bucket", "test-key")
    assert result is False
