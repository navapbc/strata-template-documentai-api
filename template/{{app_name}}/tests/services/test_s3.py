"""Tests for S3 Service methods"""
import pytest
from unittest.mock import MagicMock, patch
from services import s3 as s3_service


@pytest.fixture(autouse=True)
def mock_s3_client():
    with patch("services.s3.AWSClientFactory.get_s3_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client  # MISSING - need to return the mock
        yield mock_client


def mock_s3_body(data: bytes):
    """Helper to mock S3 streaming body"""
    mock_body = MagicMock()
    mock_body.read.return_value = data
    return mock_body


def test_upload_file_args(mock_s3_client):
    """Upload file to S3"""
    mock_file = MagicMock()
    s3_service.upload_file("bucket", "key", mock_file, content_type="text/plain", metadata={"foo": "bar"})
    mock_s3_client.upload_fileobj.assert_called_once_with(
        mock_file, "bucket", "key", 
        ExtraArgs={"ContentType": "text/plain", "Metadata": {"foo": "bar"}}
    )


def test_upload_file_no_args(mock_s3_client):
    """Upload file without content type or metadata"""
    mock_file = MagicMock()
    s3_service.upload_file("bucket", "key", mock_file)
    mock_s3_client.upload_fileobj.assert_called_once_with(
        mock_file, "bucket", "key", ExtraArgs={}
    )


def test_get_object(mock_s3_client):
    """Get object from S3"""
    mock_s3_client.get_object.return_value = {"Body": mock_s3_body(b"data")}
    result = s3_service.get_object("bucket", "key")
    mock_s3_client.get_object.assert_called_once_with(Bucket="bucket", Key="key")
    assert result["Body"].read() == b"data"


def test_head_object(mock_s3_client):
    """Get object metadata from S3"""
    mock_s3_client.head_object.return_value = {
        "ContentLength": 12345,
        "ContentType": "application/pdf",
    }
    
    result = s3_service.head_object("bucket", "key")
    mock_s3_client.head_object.assert_called_once_with(Bucket="bucket", Key="key")
    assert result == mock_s3_client.head_object.return_value


def test_put_object(mock_s3_client):
    """Put object to S3 with content type"""
    s3_service.put_object("bucket", "key", b"data", content_type="text/plain")
    
    mock_s3_client.put_object.assert_called_once_with(
        Bucket="bucket", Key="key", Body=b"data", ContentType="text/plain"
    )


def test_put_object_no_content_type(mock_s3_client):
    """Put object to S3 without content type"""
    s3_service.put_object("bucket", "key", b"data")
    mock_s3_client.put_object.assert_called_once_with(
        Bucket="bucket", Key="key", Body=b"data"
    )


def test_get_content_type(mock_s3_client):
    """Get file content type"""
    mock_s3_client.head_object.return_value = {"ContentType": "application/pdf"}
    result = s3_service.get_content_type("bucket", "key")
    assert result == "application/pdf"


def test_get_content_type_default(mock_s3_client):
    """Get file content type with default fallback"""
    mock_s3_client.head_object.return_value = {}
    result = s3_service.get_content_type("bucket", "key")
    assert result == "application/octet-stream"


def test_get_file_size_bytes(mock_s3_client):
    """Get file size in bytes"""
    mock_s3_client.head_object.return_value = {"ContentLength": 12345}
    result = s3_service.get_file_size_bytes("bucket", "key")
    assert result == 12345


def test_get_file_bytes(mock_s3_client):
    """Get file content as bytes"""
    mock_s3_client.get_object.return_value = {"Body": mock_s3_body(b"file content")}
    result = s3_service.get_file_bytes("bucket", "key")
    assert result == b"file content"


def test_is_password_protected_true(mock_s3_client):
    """Check if PDF is password protected - encrypted"""
    mock_s3_client.head_object.return_value = {"ContentType": "application/pdf"}
    mock_s3_client.get_object.return_value = {"Body": mock_s3_body(b"/Encrypt some pdf data")}
    result = s3_service.is_password_protected("bucket", "key")
    assert result is True


def test_is_password_protected_false(mock_s3_client):
    """Check if PDF is password protected - not encrypted"""
    mock_s3_client.head_object.return_value = {"ContentType": "application/pdf"}
    mock_s3_client.get_object.return_value = {"Body": mock_s3_body(b"normal pdf data")}
    result = s3_service.is_password_protected("bucket", "key")
    assert result is False


def test_is_password_protected_not_pdf(mock_s3_client):
    """Check if non-PDF is password protected - returns False"""
    mock_s3_client.head_object.return_value = {"ContentType": "image/jpeg"}
    result = s3_service.is_password_protected("bucket", "key")
    assert result is False
