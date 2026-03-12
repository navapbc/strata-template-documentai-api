import asyncio
import json
import os
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Annotated

import magic
from fastapi import FastAPI, File, Form, Header, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute

from documentai_api.config.constants import (
    API_DESCRIPTION,
    API_TITLE,
    API_VERSION,
    PROCESSING_STATUS_COMPLETED,
    S3_METADATA_KEY_BATCH_ID,
    S3_METADATA_KEY_JOB_ID,
    S3_METADATA_KEY_ORIGINAL_FILE_NAME,
    S3_METADATA_KEY_TRACE_ID,
    S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY,
    SUPPORTED_CONTENT_TYPES,
    BatchStatus,
    DocumentCategory,
    ProcessStatus,
)
from documentai_api.schemas.batch import Batch
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import s3 as s3_service
from documentai_api.utils import env
from documentai_api.utils.ddb import ClassificationData, classify_as_failed, get_ddb_by_job_id
from documentai_api.utils.logger import get_logger
from documentai_api.utils.s3 import parse_s3_uri
from documentai_api.utils.schemas import get_all_schemas, get_document_schema

logger = get_logger(__name__)
DOCUMENTAI_INPUT_LOCATION = os.getenv(env.DOCUMENTAI_INPUT_LOCATION)
DOCUMENTAI_PREPROCESSING_LOCATION = os.getenv(env.DOCUMENTAI_PREPROCESSING_LOCATION)

_max_batch_size = os.getenv(env.DOCUMENTAI_MAX_BATCH_SIZE)
DOCUMENTAI_MAX_BATCH_SIZE = int(_max_batch_size) if _max_batch_size else None

CONFIG_EXCLUDED_ROUTES = {"//config", "/openapi.json", "/docs", "/redoc", "/"}

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


def discover_endpoints(app):
    endpoints = {}

    for route in app.routes:
        if isinstance(route, APIRoute) and route.name and route.path not in CONFIG_EXCLUDED_ROUTES:
            endpoints[route.name] = route.path

    return dict(sorted(endpoints.items()))


@app.get("/")
def root():
    return {"message": API_TITLE, "status": "healthy"}


@app.get("/health")
async def health():
    return {"message": "healthy"}


@app.get("/config", name="config")
def get_config(request: Request):
    endpoints = discover_endpoints(app)
    endpoints["uploadSync"] = f"{endpoints['upload']}?wait=true"

    return {
        "apiUrl": f"{request.url.scheme}://{request.url.netloc}",
        "version": API_VERSION,
        "imageTag": os.getenv("IMAGE_TAG"),
        "environment": os.getenv("ENVIRONMENT", "local"),
        "endpoints": endpoints,
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


def validate_batch_id(batch_id: str) -> None:
    """Validate batch ID is not already in use.

    Raises:
        HTTPException: 409 if batch already exists
    """
    from documentai_api.utils.ddb import get_batch

    existing_batch = get_batch(batch_id)
    if not existing_batch:
        return

    raise HTTPException(status_code=409, detail="Batch ID already exists")


async def upload_document_for_processing(
    file: UploadFile,
    original_file_name: str,
    unique_file_name: str,
    content_type: str,
    s3_location: str | None = None,
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
        },
    )
    if not DOCUMENTAI_INPUT_LOCATION:
        raise ValueError("DOCUMENTAI_INPUT_LOCATION environment variable not set")

    try:
        metadata = {}
        if user_provided_document_category:
            # add type check for safety
            if not isinstance(user_provided_document_category, DocumentCategory):
                raise ValueError(
                    f"Expected DocumentCategory, got {type(user_provided_document_category)}"
                )

            metadata[S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY] = (
                user_provided_document_category.value
            )

        metadata[S3_METADATA_KEY_ORIGINAL_FILE_NAME] = original_file_name

        if job_id:
            metadata[S3_METADATA_KEY_JOB_ID] = job_id

        if trace_id:
            metadata[S3_METADATA_KEY_TRACE_ID] = trace_id

        if batch_id:
            metadata[S3_METADATA_KEY_BATCH_ID] = batch_id

        logger.debug(
            "S3: Starting actual upload",
            extra={
                "metadata": metadata,
                "file": file.file,
                "unique_file_name": unique_file_name,
            },
        )

        input_bucket_name, object_key = parse_s3_uri(
            f"{DOCUMENTAI_INPUT_LOCATION}/{unique_file_name}"
        )

        # pdfs and tiffs can have multiple pages
        # 1. upload to preprocessing
        # 2. check page count
        # 3. trim excess pages if needed (performance reasons, first N pages are good enough)
        # 4. upload to input
        if content_type in ["application/pdf", "image/tiff"]:
            from io import BytesIO

            from documentai_api.utils.document_detector import (
                MULTIPAGE_DETECTION_MAX_PAGES,
                DocumentDetector,
            )

            file_content = await file.read()
            detector = DocumentDetector()
            page_count = detector.get_page_count(file_content)
            logger.info(f"{original_file_name}: detected {page_count} pages")

            if page_count > MULTIPAGE_DETECTION_MAX_PAGES:
                # save original document to preprocessing
                preprocessing_path = (
                    f"{DOCUMENTAI_PREPROCESSING_LOCATION}/{batch_id}/{unique_file_name}"
                    if batch_id
                    else f"{DOCUMENTAI_PREPROCESSING_LOCATION}/{unique_file_name}"
                )
                preprocessing_bucket, preprocessing_key = parse_s3_uri(preprocessing_path)
                s3_service.upload_file(
                    preprocessing_bucket,
                    preprocessing_key,
                    BytesIO(file_content),
                    content_type,
                    metadata,
                )
                logger.info(f"Uploaded {original_file_name} to preprocessing: {preprocessing_path}")

                # truncate document to N-number of pageges
                logger.info(
                    f"Truncating {original_file_name} from {page_count} to {MULTIPAGE_DETECTION_MAX_PAGES} pages"
                )
                file_content = detector.truncate_to_pages(
                    file_content, MULTIPAGE_DETECTION_MAX_PAGES
                )

            # upload final version to input bucket to trigger processing
            input_bucket, input_key = parse_s3_uri(
                f"{DOCUMENTAI_INPUT_LOCATION}/{unique_file_name}"
            )
            s3_service.upload_file(
                input_bucket, input_key, BytesIO(file_content), content_type, metadata
            )
            logger.info(f"Uploaded {original_file_name} to input location for processing")
        else:
            # upload non-pdfs directly to input
            s3_service.upload_file(input_bucket_name, object_key, file.file, content_type, metadata)

        logger.info(f"S3 UPLOAD SUCCESS: {original_file_name}")

    except Exception as e:
        logger.error(f"Error uploading {original_file_name} to S3: {e}")
        logger.info(f"S3 UPLOAD FAILED: {original_file_name} - {e}")

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


@app.post("/v1/documents", name="upload")
async def create_document(
    request: Request,
    response: Response,
    file: Annotated[UploadFile, File(description="Document to upload")],
    category: Annotated[
        DocumentCategory | None, Form(description="Type of document being uploaded")
    ] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    wait: bool = False,  # async by default
    timeout: int = 180,  # accounts for ECS cold starts and BDA processing time
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
        original_file_name=file.filename,
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


@app.get("/v1/documents/{job_id}", name="getUploadstatus")
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


@app.get("/v1/schemas", name="listSchemas")
async def list_schemas():
    """List all supported document types."""
    schemas = get_all_schemas()
    return {"schemas": list(schemas.keys())}


@app.get("/v1/schemas/{document_type}", name="getSchemaDetail")
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

    logger.info(f"Batch {batch_id}: Starting to process {len(files)} files")

    for idx, file in enumerate(files):
        try:
            logger.info(
                f"Batch {batch_id}: Processing file {idx + 1}/{len(files)}: {file.filename}"
            )

            file_extension = file.filename.split(".")[-1]
            file_name = file.filename.split(".")[0]
            unique_file_name = f"{idx}-{file_name}-{uuid.uuid4()}.{file_extension}"
            job_id = str(uuid.uuid4())
            actual_content_type = await validate_file_type(file)
            file.file.seek(0)

            await upload_document_for_processing(
                file=file,
                original_file_name=file.filename,
                unique_file_name=unique_file_name,
                content_type=actual_content_type,
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
        except Exception as e:
            logger.error(
                f"Batch {batch_id}: Failed to process file {idx + 1}/{len(files)} ({file.filename}): {e}",
                exc_info=True,
            )
            raise

    logger.info(f"Batch {batch_id}: Completed processing all {len(files)} files")
    return job_ids


@app.post("/v1/documents/batch", name="batchUpload")
async def upload_document_batch(
    response: Response,
    files: Annotated[list[UploadFile], File(..., description="Documents to process")],
    batch_id: Annotated[str | None, Form()] = None,
    category: Annotated[DocumentCategory | None, Form()] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
):
    """Upload multiple documents as a batch."""
    from documentai_api.utils.ddb import create_batch, get_batch, update_batch_status

    if not DOCUMENTAI_MAX_BATCH_SIZE:
        raise ValueError(f"{env.DOCUMENTAI_MAX_BATCH_SIZE} environment variable not set")

    if not trace_id:
        trace_id = str(uuid.uuid4())
    if not batch_id:
        batch_id = str(uuid.uuid4())
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    if len(files) > DOCUMENTAI_MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size exceeds maximum of {DOCUMENTAI_MAX_BATCH_SIZE} files",
        )

    validate_batch_id(batch_id)

    try:
        create_batch(batch_id, len(files), category, status=BatchStatus.UPLOADING)

        job_ids = await process_batch_files(
            files=files,
            batch_id=batch_id,
            category=category,
            trace_id=trace_id,
        )

        update_batch_status(batch_id, status=BatchStatus.PROCESSING)

        response.headers["X-Trace-ID"] = trace_id
        batch_record = get_batch(batch_id)
        return {
            "batchId": batch_id,
            "batchStatus": BatchStatus.PROCESSING.value,
            "totalFiles": len(files),
            "createdAt": batch_record.get(Batch.CREATED_AT) if batch_record else None,
            "jobs": job_ids,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading batch: {e}")
        update_batch_status(batch_id, status=BatchStatus.FAILED, error_message=str(e))
        raise HTTPException(status_code=500, detail="Failed to upload batch") from e


@app.post("/v1/documents/batch/zip", name="batchUploadZip")
async def upload_zip_batch(
    response: Response,
    zip_file: Annotated[
        UploadFile,
        File(..., description="ZIP file containing documents", media_type="application/zip"),
    ],
    batch_id: Annotated[str | None, Form()] = None,
    category: Annotated[DocumentCategory | None, Form()] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
):
    """Upload a zip file containing multiple documents."""
    from documentai_api.utils.ddb import create_batch, get_batch, update_batch_status
    from documentai_api.utils.zip import extract_files_from_zip

    if not DOCUMENTAI_MAX_BATCH_SIZE:
        raise ValueError(f"{env.DOCUMENTAI_MAX_BATCH_SIZE} environment variable not set")

    if not trace_id:
        trace_id = str(uuid.uuid4())

    if not batch_id:
        batch_id = str(uuid.uuid4())

    validate_batch_id(batch_id)

    try:
        files = await extract_files_from_zip(zip_file)

        if not files:
            raise HTTPException(status_code=400, detail="No valid files found in zip")

        if len(files) > DOCUMENTAI_MAX_BATCH_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"Batch size exceeds maximum of {DOCUMENTAI_MAX_BATCH_SIZE} files",
            )

        create_batch(batch_id, len(files), category, status=BatchStatus.UPLOADING)

        job_ids = await process_batch_files(
            files=files,
            batch_id=batch_id,
            category=category,
            trace_id=trace_id,
        )

        update_batch_status(batch_id, status=BatchStatus.PROCESSING)

        response.headers["X-Trace-ID"] = trace_id
        batch_record = get_batch(batch_id)
        return {
            "batchId": batch_id,
            "batchStatus": BatchStatus.PROCESSING.value,
            "totalFiles": len(files),
            "createdAt": batch_record.get(Batch.CREATED_AT) if batch_record else None,
            "jobs": job_ids,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading zip batch: {e}")
        update_batch_status(batch_id, status=BatchStatus.FAILED, error_message=str(e))
        raise HTTPException(status_code=500, detail="Failed to upload zip batch") from e


@app.get("/v1/batches/{batch_id}", name="batchUploadStatus")
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
                "fileName": record.get(DocumentMetadata.ORIGINAL_FILE_NAME),
                "jobId": record.get(DocumentMetadata.JOB_ID),
                "jobStatus": record.get(DocumentMetadata.PROCESS_STATUS, "not_found"),
                "documentClass": record.get(DocumentMetadata.BDA_MATCHED_DOCUMENT_CLASS),
                "errorMessage": record.get(DocumentMetadata.ERROR_MESSAGE),
            }
            for record in job_records
        ]

        classification_summary = dict(
            Counter(job["documentClass"] or "unclassified" for job in jobs)
        )

        completed = sum(1 for j in jobs if j["jobStatus"] in PROCESSING_STATUS_COMPLETED)
        not_started = sum(1 for job in jobs if job["jobStatus"] == ProcessStatus.NOT_STARTED.value)
        failed = sum(1 for j in jobs if j["jobStatus"] == ProcessStatus.FAILED.value)
        current_batch_status = batch.get(Batch.BATCH_STATUS)
        completed_percentage = (
            round((completed / batch.get(Batch.TOTAL_FILES)) * 100, 1)
            if batch.get(Batch.TOTAL_FILES) > 0
            else 0
        )

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
            "totalJobsExpected": batch.get(Batch.TOTAL_FILES),
            "totalJobsSubmitted": len(jobs),
            "completedPercentage": completed_percentage,
            "completed": completed,
            "inProgress": len(jobs) - completed - not_started,
            "failed": failed,
            "createdAt": batch.get(Batch.CREATED_AT),
            "category": batch.get(Batch.CATEGORY),
            "documentClassificationSummary": classification_summary,
            "jobs": jobs,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving batch {batch_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve batch") from e


if __name__ == "__main__":
    app()
