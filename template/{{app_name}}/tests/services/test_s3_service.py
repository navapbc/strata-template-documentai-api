"""Tests for S3 Service methods."""

from documentai_api.services import s3 as s3_service


def test_upload_file_args(s3_client, s3_bucket):
    """Upload file to S3."""
    from io import BytesIO

    file_obj = BytesIO(b"test data")
    s3_service.upload_file(
        s3_bucket.name, "test-key", file_obj, content_type="text/plain", metadata={"foo": "bar"}
    )

    obj = s3_client.head_object(Bucket=s3_bucket.name, Key="test-key")
    assert obj["ContentType"] == "text/plain"
    assert obj["Metadata"] == {"foo": "bar"}


def test_upload_file_no_args(s3_client, s3_bucket):
    """Upload file without content type or metadata."""
    from io import BytesIO

    file_obj = BytesIO(b"test data")
    s3_service.upload_file(s3_bucket.name, "test-key", file_obj)

    obj = s3_client.get_object(Bucket=s3_bucket.name, Key="test-key")
    assert obj["Body"].read() == b"test data"


def test_get_object(s3_bucket):
    """Get object from S3."""
    s3_bucket.put_object(Key="test-key", Body=b"data")

    result = s3_service.get_object(s3_bucket.name, "test-key")
    assert result["Body"].read() == b"data"


def test_head_object(s3_bucket):
    """Get object metadata from S3."""
    s3_bucket.put_object(Key="test-key", Body=b"data", ContentType="application/pdf")

    result = s3_service.head_object(s3_bucket.name, "test-key")
    assert result["ContentType"] == "application/pdf"
    assert result["ContentLength"] == 4


def test_put_object(s3_client, s3_bucket):
    """Put object to S3 with content type."""
    s3_service.put_object(s3_bucket.name, "test-key", b"data", content_type="text/plain")

    obj = s3_client.get_object(Bucket=s3_bucket.name, Key="test-key")
    assert obj["Body"].read() == b"data"
    assert obj["ContentType"] == "text/plain"


def test_put_object_no_content_type(s3_client, s3_bucket):
    """Put object to S3 without content type."""
    s3_service.put_object(s3_bucket.name, "test-key", b"data")

    obj = s3_client.get_object(Bucket=s3_bucket.name, Key="test-key")
    assert obj["Body"].read() == b"data"


def test_get_content_type(s3_bucket):
    """Get file content type."""
    s3_bucket.put_object(Key="test-key", Body=b"data", ContentType="application/pdf")

    result = s3_service.get_content_type(s3_bucket.name, "test-key")
    assert result == "application/pdf"


def test_get_content_type_default(s3_bucket):
    """Get file content type with default fallback."""
    s3_bucket.put_object(Key="test-key", Body=b"data")

    result = s3_service.get_content_type(s3_bucket.name, "test-key")
    assert result == "binary/octet-stream"


def test_get_file_size_bytes(s3_bucket):
    """Get file size in bytes."""
    s3_bucket.put_object(Key="test-key", Body=b"12345")

    result = s3_service.get_file_size_bytes(s3_bucket.name, "test-key")
    assert result == 5


def test_get_file_bytes(s3_bucket):
    """Get file content as bytes."""
    s3_bucket.put_object(Key="test-key", Body=b"file content")

    result = s3_service.get_file_bytes(s3_bucket.name, "test-key")
    assert result == b"file content"


def test_is_password_protected_true(s3_bucket):
    """Check if PDF is password protected - encrypted."""
    s3_bucket.put_object(
        Key="test-key",
        Body=b"/Encrypt some pdf data",
        ContentType="application/pdf",
    )

    result = s3_service.is_password_protected(s3_bucket.name, "test-key")
    assert result is True


def test_is_password_protected_false(s3_bucket):
    """Check if PDF is password protected - not encrypted."""
    s3_bucket.put_object(Key="test-key", Body=b"normal pdf data", ContentType="application/pdf")

    result = s3_service.is_password_protected(s3_bucket.name, "test-key")
    assert result is False


def test_is_password_protected_not_pdf(s3_bucket):
    """Check if non-PDF is password protected - returns False."""
    s3_bucket.put_object(Key="test-key", Body=b"image data", ContentType="image/jpeg")

    result = s3_service.is_password_protected(s3_bucket.name, "test-key")
    assert result is False


def test_get_last_modified_at(s3_bucket):
    """Get LastModified timestamp from S3 object."""
    from datetime import datetime

    s3_bucket.put_object(Key="test-key", Body=b"data")

    result = s3_service.get_last_modified_at(s3_bucket.name, "test-key")

    assert isinstance(result, datetime)
    assert result.tzinfo is not None  # boto3 returns tzutc() which is timezone-aware
