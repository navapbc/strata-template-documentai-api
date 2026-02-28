import asyncio
import json
import os
import uuid
from dataclasses import dataclass
from typing import Annotated

import magic
from fastapi import FastAPI, Form, Header, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from documentai_api.config.constants import (
    API_DESCRIPTION,
    API_TITLE,
    API_VERSION,
    PROCESSING_STATUS_COMPLETED,
    SUPPORTED_CONTENT_TYPES,
    UPLOAD_METADATA_KEYS,
    BatchStatus,
    DocumentCategory,
    ProcessStatus,
    S3Prefix,
)
from documentai_api.schemas.batch import Batch
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import s3 as s3_service
from documentai_api.utils.ddb import ClassificationData, classify_as_failed, get_ddb_by_job_id
from documentai_api.utils.logger import get_logger
from documentai_api.utils.schemas import get_all_schemas, get_document_schema

logger = get_logger(__name__)
DDE_INPUT_LOCATION = os.getenv("DDE_INPUT_LOCATION")

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": API_TITLE, "status": "healthy"}


@app.get("/health")
async def health():
    return {"message": "healthy"}


@app.get("/config")
def get_config(request: Request):
    return {
        "apiUrl": f"{request.url.scheme}://{request.url.netloc}",
        "version": API_VERSION,
        "imageTag": os.getenv("IMAGE_TAG"),
        "environment": os.getenv("ENVIRONMENT", "local"),
        "endpoints": {
            "upload": "/v1/documents",
            "uploadSync": "/v1/documents?wait=true",
            "batchUpload": "/v1/documents/batch",
            "batchUploadZip": "/v1/documents/batch/zip",
            "batchUploadStatus": "/v1/batches/{batch_id}",
            "status": "/v1/documents/{job_id}",
            "statusWithExtractedData": "/v1/documents/{job_id}?include_extracted_data=true",
            "schemas": "/v1/schemas",
            "schemaDetail": "/v1/schemas/{document_type}",
            "health": "/health",
        },
        "supportedFileTypes": SUPPORTED_CONTENT_TYPES,
    }


@dataclass
class JobStatus:
    """Job status data from DDB."""

    ddb_record: dict | None
    object_key: str | None
    process_status: str | None
    v1_response_json: str | None


def _get_job_status(job_id: str) -> JobStatus:
    """Get job status from DDB.

    Returns:
        JobStatus: Job status data with all fields None if job not found

    Raises:
        Exception: If DDB query fails (network, permissions, etc.)
    """
    ddb_record = get_ddb_by_job_id(job_id)

    if not ddb_record:
        return JobStatus(None, None, None, None)

    object_key = ddb_record.get(DocumentMetadata.FILE_NAME)
    process_status = ddb_record.get(DocumentMetadata.PROCESS_STATUS)
    v1_response = ddb_record.get(DocumentMetadata.V1_API_RESPONSE_JSON)

    return JobStatus(ddb_record, object_key, process_status, v1_response)


async def validate_file_type(file: UploadFile) -> str:
    """Validate file type and return content type.

    Raises HTTPException if file type is not supported.
    Returns the detected content type.
    """
    file_content = await file.read()
    actual_content_type = magic.from_buffer(file_content, mime=True)

    if actual_content_type not in SUPPORTED_CONTENT_TYPES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid file type detected '{actual_content_type}'. File must be "
                f"{', '.join(SUPPORTED_CONTENT_TYPES)}"
            ),
        )

    file.file.seek(0)  # reset file pointer for subsequent reads
    return actual_content_type


async def upload_document_for_processing(
    file: UploadFile,
    unique_file_name: str,
    content_type: str,
    s3_prefix: S3Prefix = S3Prefix.INPUT,
    user_provided_document_category: DocumentCategory = None,
    job_id: str | None = None,
    trace_id: str | None = None,
    batch_id: str | None = None,
):
    logger.debug(
        "S3 upload started",
        extra={
            "unique_file_name": unique_file_name,
            "user_provided_document_category": user_provided_document_category,
            "category_type": type(user_provided_document_category).__name__,
            "s3_prefix": s3_prefix,
        },
    )
    if not DDE_INPUT_LOCATION:
        raise ValueError("DDE_INPUT_LOCATION environment variable not set")

    bucket_name = DDE_INPUT_LOCATION.replace("s3://", "")
    unique_file_name = f"{s3_prefix.value}/{unique_file_name}"

    try:
        metadata = {}

        if user_provided_document_category:
            metadata[UPLOAD_METADATA_KEYS["user_provided_document_category"]] = (
                user_provided_document_category.value
            )

        if s3_prefix:
            metadata[UPLOAD_METADATA_KEYS["s3_prefix"]] = s3_prefix.value

        if job_id:
            metadata[UPLOAD_METADATA_KEYS["job_id"]] = job_id

        if trace_id:
            metadata[UPLOAD_METADATA_KEYS["trace_id"]] = trace_id

        if batch_id:
            metadata[UPLOAD_METADATA_KEYS["batch_id"]] = batch_id

        logger.debug(
            "S3: Starting actual upload",
            extra={
                "metadata": metadata,
                "file": file.file,
                "document_upload_bucket_name": bucket_name,
                "unique_file_name": unique_file_name,
            },
        )

        s3_service.upload_file(bucket_name, unique_file_name, file.file, content_type, metadata)
        logger.info("=== S3 UPLOAD SUCCESS ===")

    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        logger.info(f"=== S3 UPLOAD FAILED: {e} ===")
        raise HTTPException(
            status_code=500,
            detail="Document upload failed",
        ) from e


async def get_v1_document_processing_results(job_id: str, timeout: int) -> dict:
    """Poll for document processing completion with timeout."""
    elapsed_time = 0
    object_key = None
    polling_interval = 5

    while elapsed_time < timeout:
        try:
            job_status = _get_job_status(job_id)

            if job_status.object_key:
                object_key = job_status.object_key

            # processing complete, return results
            if (
                job_status.process_status in PROCESSING_STATUS_COMPLETED
                and job_status.v1_response_json
            ):
                return json.loads(job_status.v1_response_json)

            # still processing, wait and poll again
            await asyncio.sleep(polling_interval)
            elapsed_time += polling_interval

        except Exception as e:
            msg = f"Error polling DynamoDB for job {job_id}: {e}"
            logger.error(msg)

            await asyncio.sleep(polling_interval)
            elapsed_time += polling_interval

    # timeout - update ddb with failure if we have object_key
    if object_key:
        return classify_as_failed(
            object_key=object_key,
            error_message="Processing timeout",
            data=ClassificationData(
                additional_info=f"Processing did not complete within {timeout} seconds"
            ),
        )
    else:
        # fallback if we never got a record
        return {
            "jobStatus": "failed",
            "message": f"Processing timeout after {timeout} seconds",
            "processedAt": None,
        }


@app.post("/v1/documents")
async def create_document(
    request: Request,
    response: Response,
    file: UploadFile,
    category: Annotated[
        DocumentCategory | None, Form(description="Type of document being uploaded")
    ] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    wait: bool = False,  # async by default
    timeout: int = 120,  # optional timeout for synchronous processing, only used when wait=true
):
    """Upload a document for processing.

    Args:
        wait: If true, waits for processing to complete before returning results.
              If false (default), returns immediately with job_id for async polling.
        timeout: Maximum seconds to wait when wait=true (default: 120)
    """
    if not trace_id:
        trace_id = str(uuid.uuid4())

    actual_content_type = await validate_file_type(file)
    logger.info(
        f"Processing {file.filename}; category: {category}; content-type: {actual_content_type}"
    )

    file.file.seek(0)
    file_extension = file.filename.split(".")[-1]
    file_name = file.filename.split(".")[0]
    unique_file_name = f"{file_name}-{uuid.uuid4()}.{file_extension}"
    job_id = str(uuid.uuid4())

    await upload_document_for_processing(
        file=file,
        unique_file_name=unique_file_name,
        content_type=actual_content_type,
        user_provided_document_category=category,
        job_id=job_id,
        trace_id=trace_id,
    )

    response.headers["X-Trace-ID"] = trace_id
    if not wait:
        return {
            "jobId": job_id,
            "jobStatus": ProcessStatus.NOT_STARTED.value,
            "message": "Document uploaded successfully",
        }
    else:
        results = await get_v1_document_processing_results(job_id, timeout)
        return results


@app.get("/v1/documents/{job_id}")
async def get_document_results(job_id: str, include_extracted_data: bool = False):
    """Get processing results by job ID."""
    try:
        job_status = _get_job_status(job_id)

        if not job_status.ddb_record:
            raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found")

        if not job_status.v1_response_json:
            return {
                "jobId": job_id,
                "jobStatus": job_status.process_status,
                "message": "Processing in progress",
            }

        # processing complete
        if include_extracted_data:
            # rebuild response with extracted data
            from documentai_api.utils.response_builder import build_v1_api_response

            return build_v1_api_response(
                object_key=job_status.object_key,
                status=job_status.process_status,
                include_extracted_data=True,
            )
        else:
            # return cached response without extracted data
            return json.loads(job_status.v1_response_json)

    except HTTPException:
        raise
    except Exception as e:
        msg = f"Error retrieving results for job {job_id}: {e}"
        logger.error(msg)
        raise HTTPException(status_code=500, detail="Failed to retrieve results") from e


@app.get("/v1/schemas")
async def list_schemas():
    """List all supported document types."""
    schemas = get_all_schemas()
    return {"schemas": list(schemas.keys())}


@app.get("/v1/schemas/{document_type}")
async def get_schema(document_type: str):
    """Get field schema for a specific document type."""
    schema = get_document_schema(document_type)

    if not schema:
        raise HTTPException(
            status_code=404, detail=f"Schema not found for document type: {document_type}"
        )

    return schema


async def process_batch_files(
    files: list[UploadFile],
    batch_id: str,
    category: DocumentCategory | None,
    trace_id: str,
) -> list[dict]:
    """Process multiple files for batch upload.

    Returns list of job info dicts with fileName, jobId, batchPosition.
    """
    job_ids = []

    for idx, file in enumerate(files):
        file_extension = file.filename.split(".")[-1]
        file_name = file.filename.split(".")[0]
        unique_file_name = f"{batch_id}/{idx}-{file_name}-{uuid.uuid4()}.{file_extension}"
        job_id = str(uuid.uuid4())
        actual_content_type = await validate_file_type(file)
        file.file.seek(0)

        await upload_document_for_processing(
            file=file,
            unique_file_name=unique_file_name,
            content_type=actual_content_type,
            s3_prefix=S3Prefix.BATCHES,
            user_provided_document_category=category,
            job_id=job_id,
            trace_id=trace_id,
            batch_id=batch_id,
        )

        job_ids.append(
            {
                "fileName": file.filename,
                "jobId": job_id,
                "batchPosition": idx,
            }
        )

    return job_ids


@app.post("/v1/documents/batch")
async def upload_document_batch(
    response: Response,
    files: list[UploadFile],
    batch_id: Annotated[str | None, Form()] = None,
    category: Annotated[DocumentCategory | None, Form()] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
):
    """Upload multiple documents as a batch."""
    from documentai_api.utils.ddb import create_batch, update_batch_status

    if not trace_id:
        trace_id = str(uuid.uuid4())
    if not batch_id:
        batch_id = str(uuid.uuid4())
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    try:
        create_batch(batch_id, len(files), category, status=BatchStatus.UPLOADING)
        job_ids = await process_batch_files(files, batch_id, category, trace_id)
        update_batch_status(batch_id, status=BatchStatus.PROCESSING)

        response.headers["X-Trace-ID"] = trace_id
        return {
            "batchId": batch_id,
            "batchStatus": BatchStatus.PROCESSING.value,
            "totalFiles": len(files),
            "jobs": job_ids,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading batch: {e}")
        update_batch_status(batch_id, status=BatchStatus.FAILED, error_message=str(e))
        raise HTTPException(status_code=500, detail="Failed to upload batch") from e


@app.post("/v1/documents/batch/zip")
async def upload_zip_batch(
    response: Response,
    zip_file: UploadFile,
    batch_id: Annotated[str | None, Form()] = None,
    category: Annotated[DocumentCategory | None, Form()] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
):
    """Upload a zip file containing multiple documents."""
    from documentai_api.utils.ddb import create_batch, update_batch_status
    from documentai_api.utils.zip import extract_files_from_zip

    if not trace_id:
        trace_id = str(uuid.uuid4())
    if not batch_id:
        batch_id = str(uuid.uuid4())

    try:
        files = await extract_files_from_zip(zip_file)

        if not files:
            raise HTTPException(status_code=400, detail="No valid files found in zip")

        create_batch(batch_id, len(files), category, status=BatchStatus.UPLOADING)
        job_ids = await process_batch_files(files, batch_id, category, trace_id)
        update_batch_status(batch_id, status=BatchStatus.PROCESSING)

        response.headers["X-Trace-ID"] = trace_id
        return {
            "batchId": batch_id,
            "batchStatus": BatchStatus.PROCESSING.value,
            "totalFiles": len(files),
            "jobs": job_ids,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading zip batch: {e}")
        update_batch_status(batch_id, status=BatchStatus.FAILED, error_message=str(e))
        raise HTTPException(status_code=500, detail="Failed to upload zip batch") from e


@app.get("/v1/batches/{batch_id}")
async def get_batch_status(batch_id: str):
    """Get status of all documents in a batch.

    Note: Batch completion is lazily evaluated. When all jobs are complete,
    this endpoint updates the batch status to COMPLETED. For real-time updates,
    consider implementing an event-driven approach with DDB Streams or EventBridge.
    """
    from documentai_api.utils.ddb import get_batch, query_jobs_by_batch_id, update_batch_status

    try:
        batch = get_batch(batch_id)

        if not batch:
            raise HTTPException(status_code=404, detail=f"Batch {batch_id} not found")

        job_records = query_jobs_by_batch_id(batch_id)
        jobs = [
            {
                "fileName": record.get("fileName"),
                "jobId": record.get("jobId"),
                "jobStatus": record.get("processStatus", "not_found"),
            }
            for record in job_records
        ]

        completed = sum(1 for j in jobs if j["jobStatus"] in PROCESSING_STATUS_COMPLETED)
        failed = sum(1 for j in jobs if j["jobStatus"] == ProcessStatus.FAILED.value)
        current_batch_status = batch.get(Batch.BATCH_STATUS)

        # lazy completion check: if all jobs are done and batch is still "processing",
        # update batch status to "completed" in DDB
        if (
            current_batch_status == BatchStatus.PROCESSING.value
            and len(jobs) > 0
            and completed == len(jobs)
        ):
            update_batch_status(batch_id, status=BatchStatus.COMPLETED)
            current_batch_status = BatchStatus.COMPLETED.value
            logger.info(f"Batch {batch_id} marked as completed ({completed}/{len(jobs)} jobs done)")

        return {
            "batchId": batch_id,
            "batchStatus": current_batch_status,
            "totalJobs": len(jobs),
            "completed": completed,
            "inProgress": len(jobs) - completed,
            "failed": failed,
            "createdAt": batch.get(Batch.CREATED_AT),
            "category": batch.get(Batch.CATEGORY),
            "jobs": jobs,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving batch {batch_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve batch") from e


if __name__ == "__main__":
    app()
