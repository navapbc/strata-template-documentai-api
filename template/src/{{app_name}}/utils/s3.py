
import asyncio
import logging
import os
from fastapi import HTTPException, UploadFile
from {{app_name}}.config.settings import (
    UPLOAD_METADATA_KEYS,
    DocumentCategory
)
from functools import wraps
from typing import Any, Callable, Dict

from {{app_name}}.services import s3 as s3_service

DDE_INPUT_LOCATION = os.getenv("DDE_INPUT_LOCATION")

def validate_s3_event(handler_func: Callable) -> Callable:
    """Decorator to validate S3 event structure before processing."""

    @wraps(handler_func)
    def wrapper(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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
    """Extract file key and bucket name from EventBridge event"""
    try:
        file_key = event["detail"]["object"]["key"]
        bucket_name = event["detail"]["bucket"]["name"]

        if include_metadata:
            metadata_response = s3_service.head_object(bucket_name, file_key)
            metadata = metadata_response.get("Metadata", {})
            return file_key, bucket_name, metadata

        return file_key, bucket_name
    except (KeyError, TypeError):
        raise ValueError("Invalid EventBridge event structure")


async def upload_document_for_processing(
    file: UploadFile,
    unique_file_name: str,
    content_type: str,
    user_provided_document_category: DocumentCategory = None,
    job_id: str = None,
    trace_id: str = None
):
    print("=== S3 UPLOAD STARTED ===")
    print(f"DEBUG S3: user_provided_document_category = {repr(user_provided_document_category)}")
    print(f"DEBUG S3: type = {type(user_provided_document_category)}")
    if not DDE_INPUT_LOCATION:
        raise ValueError("DDE_INPUT_LOCATION environment variable not set")

    bucket_name = DDE_INPUT_LOCATION.replace("s3://", "")

    try:
        metadata = {}
        if user_provided_document_category:
            # add type check for safety
            if not isinstance(user_provided_document_category, DocumentCategory):
                raise ValueError(
                    f"Expected DocumentCategory, got {type(user_provided_document_category)}"
                )

            print(f"DEBUG S3: Converting to string: {str(user_provided_document_category)}")
            metadata[UPLOAD_METADATA_KEYS["user_provided_document_category"]] = (
                user_provided_document_category.value
            )

        if job_id:
            metadata[UPLOAD_METADATA_KEYS["job_id"]] = job_id

        if trace_id:
            metadata[UPLOAD_METADATA_KEYS["trace_id"]] = trace_id

        print(f"DEBUG S3: About to upload with metadata: {metadata}")
        print(f"DEBUG S3: file.file = {file.file}")
        print(f"DEBUG S3: document_upload_bucket_name = {repr(bucket_name)}")
        print(f"DEBUG S3: unique_file_name = {repr(unique_file_name)}")

        s3_service.upload_file(bucket_name, unique_file_name, file.file, content_type, metadata)
        print("=== S3 UPLOAD SUCCESS ===")

    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        print(f"=== S3 UPLOAD FAILED: {e} ===")
        raise HTTPException(
            status_code=500,
            detail="Document upload failed",
        )