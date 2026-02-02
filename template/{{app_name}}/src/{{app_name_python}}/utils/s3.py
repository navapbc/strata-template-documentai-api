from collections.abc import Callable
from functools import wraps
from typing import Any

from services import s3 as s3_service


def validate_s3_event(handler_func: Callable) -> Callable:
    """Decorator to validate S3 event structure before processing."""

    @wraps(handler_func)
    def wrapper(event: dict[str, Any], context: Any) -> dict[str, Any]:
        # validate required S3 event structure
        if not event.get("detail"):
            raise ValueError("Missing 'detail' in event")

        detail = event["detail"]
        if not detail.get("object", {}).get("key"):
            raise ValueError("Missing object key in S3 event")

        if not detail.get("bucket", {}).get("name"):
            raise ValueError("Missing bucket name in S3 event")

        return handler_func(event, context)

    return wrapper


__all__ = ["validate_s3_event"]


def extract_s3_info_from_event(event, include_metadata=False):
    """Extract file key and bucket name from EventBridge event."""
    try:
        file_key = event["detail"]["object"]["key"]
        bucket_name = event["detail"]["bucket"]["name"]

        if include_metadata:
            metadata_response = s3_service.head_object(bucket_name, file_key)
            metadata = metadata_response.get("Metadata", {})
            return file_key, bucket_name, metadata

        return file_key, bucket_name
    except (KeyError, TypeError) as e:
        raise ValueError("Invalid EventBridge event structure") from e
