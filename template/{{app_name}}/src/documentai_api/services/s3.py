"""S3 Service methods."""

from datetime import datetime

from documentai_api.utils.aws_client_factory import AWSClientFactory


def upload_file(
    bucket: str, key: str, file_obj, content_type: str | None = None, metadata: dict | None = None
) -> None:
    """Upload file to S3."""
    s3_client = AWSClientFactory.get_s3_client()

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    if metadata:
        extra_args["Metadata"] = metadata

    s3_client.upload_fileobj(file_obj, bucket, key, ExtraArgs=extra_args)


def get_object(bucket: str, key: str) -> dict:
    """Get object from S3."""
    s3_client = AWSClientFactory.get_s3_client()
    return s3_client.get_object(Bucket=bucket, Key=key)


def head_object(bucket: str, key: str) -> dict:
    """Get object metadata from S3."""
    s3_client = AWSClientFactory.get_s3_client()
    return s3_client.head_object(Bucket=bucket, Key=key)


def put_object(bucket: str, key: str, body: bytes, content_type: str | None = None) -> None:
    """Put object to S3."""
    s3_client = AWSClientFactory.get_s3_client()

    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    s3_client.put_object(Bucket=bucket, Key=key, Body=body, **extra_args)


def get_content_type(bucket: str, key: str) -> str:
    """Get file content type."""
    response = head_object(bucket, key)
    return response.get("ContentType", "application/octet-stream")


def get_file_size_bytes(bucket: str, key: str) -> int:
    """Get file size in bytes."""
    response = head_object(bucket, key)
    return response.get("ContentLength", 0)


def get_file_bytes(bucket: str, key: str) -> bytes:
    """Get file content as bytes."""
    response = get_object(bucket, key)
    return response["Body"].read()


def is_password_protected(bucket: str, key: str) -> bool:
    """Check if PDF is password protected."""
    content_type = get_content_type(bucket, key)

    if content_type in ["application/pdf", "binary/octet-stream"]:
        file_bytes = get_file_bytes(bucket, key)
        return b"/Encrypt" in file_bytes[:2048]

    return False


def get_last_modified_at(bucket: str, key: str) -> datetime:
    """Get object's last modified timestamp."""
    response = head_object(bucket, key)
    return response["LastModified"]


def generate_presigned_url(
    bucket: str, key: str, content_type: str, metadata: dict, expiration: int = 3600
) -> str:
    """Generate a presigned URL for PUT operation.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        content_type: Content type for the upload
        metadata: S3 metadata dictionary
        expiration: URL expiration time in seconds

    Returns:
        Presigned URL as string
    """
    s3_client = AWSClientFactory.get_s3_client()

    params = {
        "Bucket": bucket,
        "Key": key,
        "ContentType": content_type,
        "Metadata": metadata,
    }

    return s3_client.generate_presigned_url("put_object", Params=params, ExpiresIn=expiration)
