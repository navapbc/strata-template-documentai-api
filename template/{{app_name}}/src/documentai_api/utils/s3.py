from urllib.parse import urlparse


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse S3 URI into bucket and key.

    Args:
        s3_uri: S3 URI in format s3://bucket/key

    Returns:
        Tuple of (bucket, key)
    """
    parts = urlparse(s3_uri)
    bucket_name = parts.netloc
    prefix = parts.path.lstrip("/")
    return (bucket_name, prefix)


def get_s3_prefix_from_location(s3_location: str) -> str:
    """Extract S3 prefix from location environment variable.

    Args:
        s3_location: Environment variable value (e.g. "s3://bucket/input")

    Returns:
        The prefix portion (e.g. "input"), or empty string if no prefix
    """
    if not s3_location:
        return ""

    _, prefix = parse_s3_uri(s3_location)
    return prefix
