#!/usr/bin/env python3
"""Process uploaded documents: insert to DDB, convert if needed, invoke BDA."""

import os
from typing import Any

import typer
from botocore.exceptions import ClientError
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from documentai_api.config.constants import (
    S3_METADATA_KEY_JOB_ID,
    S3_METADATA_KEY_ORIGINAL_FILE_NAME,
    S3_METADATA_KEY_TRACE_ID,
    S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY,
    ConfigDefaults,
    ProcessStatus,
)
from documentai_api.logging import get_logger
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import s3 as s3_service
from documentai_api.utils.bda_invoker import invoke_bedrock_data_automation
from documentai_api.utils.ddb import (
    classify_as_failed,
    classify_as_not_implemented,
    get_ddb_record,
    insert_initial_ddb_record,
    set_bda_processing_status_not_started,
    set_bda_processing_status_started,
)
from documentai_api.utils.env import DOCUMENTAI_INPUT_LOCATION, MAX_BDA_INVOKE_RETRY_ATTEMPTS
from documentai_api.utils.models import ClassificationData
from documentai_api.utils.s3 import parse_s3_uri

logger = get_logger(__name__)
app = typer.Typer()


def is_file_too_large_for_bda(content_type: str, file_size_bytes: int) -> bool:
    """Check if file exceeds BDA size limits based on content type."""
    if content_type in ["image/jpeg", "image/png"]:
        return int(file_size_bytes) > int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value)
    elif content_type in ["application/pdf", "image/tiff"]:
        return int(file_size_bytes) > int(ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value)
    else:
        # unknown file type, assume document limit
        return int(file_size_bytes) > int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value)


def convert_to_grayscale(
    object_key: str, file_bytes: bytes, content_type: str
) -> tuple[bytes, str]:
    """Convert image to grayscale, and to PDF if over 5MB."""
    if content_type not in ["image/jpeg", "image/png", "image/bmp", "image/tiff"]:
        return file_bytes, content_type

    try:
        import io

        import cv2
        import numpy as np
        from PIL import Image

        nparr = np.frombuffer(file_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return file_bytes, content_type

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # convert to PIL Image for size check and PDF conversion
        pil_image = Image.fromarray(gray)

        # try jpeg first
        jpeg_output = io.BytesIO()
        pil_image.save(jpeg_output, format="JPEG", quality=100)
        jpeg_bytes = jpeg_output.getvalue()

        if len(jpeg_bytes) > int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value):
            logger.info(f"{object_key} too large for BDA, converting to PDF")
            pdf_output = io.BytesIO()
            pil_image.save(pdf_output, format="PDF")
            return pdf_output.getvalue(), "application/pdf"
        else:
            return jpeg_bytes, "image/jpeg"

    except Exception as e:
        logger.error(f"Grayscale conversion failed: {e}")
        return file_bytes, content_type


def convert_s3_object_to_grayscale(bucket_name: str, object_key: str) -> bool:
    """Convert S3 image to grayscale in-place."""
    try:
        # download file
        response = s3_service.get_object(bucket_name, object_key)
        file_bytes = response["Body"].read()
        content_type = response.get("ContentType", "application/octet-stream")

        # convert to grayscale
        grayscale_bytes, content_type = convert_to_grayscale(object_key, file_bytes, content_type)

        # upload back (overwrite)
        s3_service.put_object(bucket_name, object_key, grayscale_bytes, content_type)

        # Check final size
        final_size = len(grayscale_bytes)
        if is_file_too_large_for_bda(content_type, final_size):
            logger.error(f"File still too large after conversion: {final_size} bytes")
            return False

        logger.info(f"Converted {object_key} for BDA processing")

        return True
    except Exception as e:
        logger.error(f"Failed to convert {object_key} to grayscale: {e}")
        return False


@retry(
    stop=stop_after_attempt(int(os.getenv(MAX_BDA_INVOKE_RETRY_ATTEMPTS, "3"))),
    wait=wait_exponential_jitter(initial=10, max=120),
    retry=retry_if_exception_type(ClientError),
)
def _invoke_bda(bucket_name: str, object_key: str, ddb_key: str) -> dict[str, Any]:
    """Invoke BDA for a file that's ready for processing."""
    invocation_arn = invoke_bedrock_data_automation(bucket_name, object_key)

    set_bda_processing_status_started(
        object_key=ddb_key,
        bda_invocation_arn=invocation_arn,
    )

    logger.info(f"BDA job started for {ddb_key}, ARN: {invocation_arn}")
    return {"invocationArn": invocation_arn}


def invoke_bda(bucket_name: str, object_key: str, ddb_key: str) -> dict[str, Any]:
    """Wrapper that handles retry failures."""
    try:
        return _invoke_bda(bucket_name, object_key, ddb_key)
    except RetryError as e:
        retry_state = e.last_attempt
        attempt_number = retry_state.attempt_number

        logger.error(f"BDA invocation failed for {ddb_key} after {attempt_number} attempts: {e}")
        classify_as_failed(
            object_key=ddb_key,
            error_message="BDA invocation failed",
            data=ClassificationData(additional_info=str(e)),
        )
        raise


def main(
    object_key: str,
    bucket_name: str | None = None,
    user_provided_document_category: str | None = None,
    job_id: str | None = None,
    trace_id: str | None = None,
) -> None:
    """Process uploaded document and invoke BDA.

    This job combines DDB insertion, grayscale conversion, and BDA invocation
    into a single workflow triggered by S3 upload events.

    Args:
        object_key: S3 object key (e.g. "input/document.pdf")
        bucket_name: Optional S3 bucket name (defaults to DOCUMENTAI_INPUT_LOCATION env var)
        user_provided_document_category: Optional document category (will be read from S3 metadata if not provided)
        job_id: Optional job ID (will be read from S3 metadata if not provided)
        trace_id: Optional trace ID (will be read from S3 metadata if not provided)
    """
    input_location = os.getenv(DOCUMENTAI_INPUT_LOCATION, "")

    if bucket_name is None:
        bucket_name, _ = parse_s3_uri(input_location)

    logger.info(f"Processing document: s3://{bucket_name}/{object_key}")

    response = s3_service.head_object(bucket_name, object_key)
    metadata = response.get("Metadata", {})
    original_file_name = metadata.get(S3_METADATA_KEY_ORIGINAL_FILE_NAME)
    if not original_file_name:
        logger.warning("Original file name not present in S3 metadata")
        original_file_name = ""

    if not all([job_id, trace_id, user_provided_document_category]):
        try:
            job_id = job_id or metadata.get(S3_METADATA_KEY_JOB_ID)
            trace_id = trace_id or metadata.get(S3_METADATA_KEY_TRACE_ID)
            user_provided_document_category = user_provided_document_category or metadata.get(
                S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY
            )
        except Exception as e:
            logger.warning(f"Could not read S3 metadata: {e}")

    # strip S3 prefix for DynamoDB key (files are stored without prefix)
    ddb_key = os.path.basename(object_key)

    existing_record = get_ddb_record(ddb_key)

    if existing_record is None:
        # first time seeing this file
        logger.info(f"First time processing {ddb_key}")
        insert_initial_ddb_record(
            source_bucket_name=bucket_name,
            source_object_key=object_key,
            ddb_key=ddb_key,
            original_file_name=original_file_name,
            user_provided_document_category=user_provided_document_category,
            job_id=job_id,
            trace_id=trace_id,
        )

        existing_record = get_ddb_record(ddb_key)

        if existing_record is None:
            raise Exception("Could not retrieve DDB record after creation")

    status = existing_record.get(DocumentMetadata.PROCESS_STATUS)

    if status == ProcessStatus.PENDING_GRAYSCALE_CONVERSION:
        if convert_s3_object_to_grayscale(bucket_name, object_key):
            set_bda_processing_status_not_started(ddb_key)
            invoke_bda(bucket_name, object_key, ddb_key)
            logger.info(f"Converted {ddb_key} to grayscale and invoked BDA")
        else:
            # conversion failed or file too large
            classify_as_not_implemented(
                object_key=ddb_key,
                data=ClassificationData(additional_info="File too large after conversion"),
            )
    elif status == ProcessStatus.NOT_STARTED.value:
        # ready for BDA immediately
        invoke_bda(bucket_name, object_key, ddb_key)
    else:
        logger.info(f"File {ddb_key} already has status: {status}, skipping")


@app.command()
def cli(
    object_key: str = typer.Argument(..., help="S3 object key (e.g. 'input/document.pdf')"),
    bucket_name: str | None = typer.Argument(
        None, help="S3 bucket name (defaults to DOCUMENTAI_INPUT_LOCATION env var)"
    ),
    user_provided_document_category: str | None = typer.Option(
        None, help="User-provided document category (read from S3 metadata if not provided)"
    ),
    job_id: str | None = typer.Option(None, help="Job ID (read from S3 metadata if not provided)"),
    trace_id: str | None = typer.Option(
        None, help="Trace ID (read from S3 metadata if not provided)"
    ),
) -> None:
    """Process uploaded document and invoke BDA."""
    try:
        main(object_key, bucket_name, user_provided_document_category, job_id, trace_id)
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
