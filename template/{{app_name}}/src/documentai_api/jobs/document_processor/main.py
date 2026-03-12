#!/usr/bin/env python3
"""Process uploaded documents: insert to DDB, convert if needed, invoke BDA."""

import os

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
    S3_METADATA_KEY_BATCH_ID,
    S3_METADATA_KEY_JOB_ID,
    S3_METADATA_KEY_ORIGINAL_FILE_NAME,
    S3_METADATA_KEY_TRACE_ID,
    S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY,
    ConfigDefaults,
    ProcessStatus,
)
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import s3 as s3_service
from documentai_api.utils.bda_invoker import invoke_bedrock_data_automation
from documentai_api.utils.ddb import (
    ClassificationData,
    classify_as_failed,
    classify_as_not_implemented,
    get_ddb_record,
    insert_initial_ddb_record,
    set_bda_processing_status_started,
)
from documentai_api.utils.env import DOCUMENTAI_INPUT_LOCATION, MAX_BDA_INVOKE_RETRY_ATTEMPTS
from documentai_api.utils.logger import get_logger
from documentai_api.utils.s3 import parse_s3_uri

MAX_BDA_RETRY_ATTEMPTS = int(os.getenv(MAX_BDA_INVOKE_RETRY_ATTEMPTS, "3"))

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


@retry(
    stop=stop_after_attempt(MAX_BDA_RETRY_ATTEMPTS),
    wait=wait_exponential_jitter(initial=10, max=120),
    retry=retry_if_exception_type(ClientError),
)
def _invoke_bda(bucket_name: str, object_key: str, ddb_key: str) -> dict:
    """Invoke BDA for a file that's ready for processing."""
    invocation_arn = invoke_bedrock_data_automation(bucket_name, object_key)

    set_bda_processing_status_started(
        object_key=ddb_key,
        bda_invocation_arn=invocation_arn,
    )

    logger.info(f"BDA job started for {ddb_key}, ARN: {invocation_arn}")
    return {"invocationArn": invocation_arn}


def invoke_bda(bucket_name: str, object_key: str, ddb_key: str) -> dict:
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
    batch_id: str | None = None,
):
    """Process uploaded document and invoke BDA.

    Args:
        object_key: S3 object key (e.g. "input/document.pdf")
        bucket_name: Optional S3 bucket name (defaults to DOCUMENTAI_INPUT_LOCATION env var)
        job_id: Optional job ID (will be read from S3 metadata if not provided)
        trace_id: Optional trace ID (will be read from S3 metadata if not provided)
        batch_id: Optional batch ID (will be read from S3 metadata if not provided)
    """
    input_location = os.getenv(DOCUMENTAI_INPUT_LOCATION, "")

    if bucket_name is None:
        bucket_name, _ = parse_s3_uri(input_location)

    logger.info(f"Processing document: s3://{bucket_name}/{object_key}")

    if not all([job_id, trace_id]):
        try:
            response = s3_service.head_object(bucket_name, object_key)
            metadata = response.get("Metadata", {})
            original_file_name = metadata.get(S3_METADATA_KEY_ORIGINAL_FILE_NAME)
            job_id = job_id or metadata.get(S3_METADATA_KEY_JOB_ID)
            trace_id = trace_id or metadata.get(S3_METADATA_KEY_TRACE_ID)
            batch_id = batch_id or metadata.get(S3_METADATA_KEY_BATCH_ID)
            user_provided_document_category = user_provided_document_category or metadata.get(
                S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY
            )
        except Exception as e:
            logger.warning(f"Could not read S3 metadata: {e}")

    # strip S3 prefix for DynamoDB key (files are stored without prefix)
    ddb_key = os.path.basename(object_key)

    try:
        existing_record = get_ddb_record(ddb_key)
    except ValueError:
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
            batch_id=batch_id,
        )

        existing_record = get_ddb_record(ddb_key)

    status = existing_record.get(DocumentMetadata.PROCESS_STATUS)

    if status == ProcessStatus.NOT_STARTED.value:
        # check if file is too large for BDA
        response = s3_service.head_object(bucket_name, object_key)
        file_size = response.get("ContentLength", 0)
        content_type = response.get("ContentType", "application/octet-stream")

        if is_file_too_large_for_bda(content_type, file_size):
            classify_as_not_implemented(
                object_key=ddb_key,
                data=ClassificationData(
                    additional_info=f"File too large for BDA: {file_size} bytes"
                ),
            )
            logger.error(f"File {ddb_key} too large for BDA: {file_size} bytes")
        else:
            # files are already grayscale and ready for BDA
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
    batch_id: str | None = typer.Option(
        None, help="Batch ID (read from S3 metadata if not provided)"
    ),
):
    """Process uploaded document and invoke BDA."""
    try:
        main(object_key, bucket_name, job_id, trace_id, batch_id)
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
