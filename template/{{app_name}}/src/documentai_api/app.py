import asyncio
import json
import os
import secrets
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, BinaryIO

import magic
from fastapi import (
    Depends,
    FastAPI,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader

from documentai_api.config.constants import (
    API_AUTH_KEY_HEADER_NAME,
    API_DESCRIPTION,
    API_TITLE,
    API_VERSION,
    PROCESSING_STATUS_COMPLETED,
    S3_METADATA_KEY_ORIGINAL_FILE_NAME,
    SUPPORTED_CONTENT_TYPES,
    UPLOAD_METADATA_KEYS,
    DocumentCategory,
    ProcessStatus,
)
from documentai_api.logging import get_logger
from documentai_api.models.api_responses import (
    ConfigResponse,
    HealthResponse,
    JobStatusResponse,
    SchemaDetailResponse,
    SchemaListResponse,
    UploadAsyncResponse,
)
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import s3 as s3_service
from documentai_api.utils import env
from documentai_api.utils.ddb import classify_as_failed, get_ddb_by_job_id
from documentai_api.utils.models import ClassificationData
from documentai_api.utils.s3 import parse_s3_uri
from documentai_api.utils.schemas import get_all_schemas, get_document_schema

logger = get_logger(__name__)

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


api_key_header = APIKeyHeader(name=API_AUTH_KEY_HEADER_NAME, auto_error=False)


def verify_api_key(api_key: str = Depends(api_key_header)) -> None:
    """Simple placeholder API key check."""
    expected_key = os.getenv(env.API_AUTH_INSECURE_SHARED_KEY)

    if not expected_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="API key not configured"
        )

    if not api_key or not secrets.compare_digest(api_key, expected_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


# public endpoints (no auth required)
@app.get("/")
def root() -> dict[str, Any]:
    return {"message": API_TITLE, "status": "healthy"}


@app.get("/health")
async def health() -> HealthResponse:
    return HealthResponse(message="healthy")


@app.get("/config")
def get_config(request: Request) -> ConfigResponse:
    return ConfigResponse(
        api_url=f"{request.url.scheme}://{request.url.netloc}",
        version=API_VERSION,
        image_tag=os.getenv("IMAGE_TAG"),
        environment=os.getenv("ENVIRONMENT", "local"),
        endpoints={
            "upload": "/v1/documents",
            "uploadSync": "/v1/documents?wait=true",
            "status": "/v1/documents/{job_id}",
            "statusWithExtractedData": "/v1/documents/{job_id}?include_extracted_data=true",
            "schemas": "/v1/schemas",
            "schemaDetail": "/v1/schemas/{document_type}",
            "health": "/health",
        },
        supported_file_types=list(SUPPORTED_CONTENT_TYPES),
    )


@dataclass
class JobStatus:
    """Job status data from DDB."""

    ddb_record: dict[str, Any] | None
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


async def upload_document_for_processing(
    file: BinaryIO,
    original_file_name: str,
    unique_file_name: str,
    content_type: str,
    user_provided_document_category: DocumentCategory | None = None,
    job_id: str | None = None,
    trace_id: str | None = None,
) -> None:
    logger.debug(
        "S3 upload started",
        extra={
            "unique_file_name": unique_file_name,
            "user_provided_document_category": user_provided_document_category,
            "category_type": type(user_provided_document_category).__name__,
        },
    )
    input_location = env.get_required_env(env.DOCUMENTAI_INPUT_LOCATION)

    # DOCUMENTAI_INPUT_LOCATION includes full path (e.g. s3://bucket/input)
    bucket_name, object_key = parse_s3_uri(f"{input_location}/{unique_file_name}")

    try:
        metadata = {}
        if user_provided_document_category:
            # add type check for safety
            if not isinstance(user_provided_document_category, DocumentCategory):
                raise ValueError(
                    f"Expected DocumentCategory, got {type(user_provided_document_category)}"
                )

            metadata[UPLOAD_METADATA_KEYS["user_provided_document_category"]] = (
                user_provided_document_category.value
            )

        metadata[S3_METADATA_KEY_ORIGINAL_FILE_NAME] = original_file_name

        if job_id:
            metadata[UPLOAD_METADATA_KEYS["job_id"]] = job_id

        if trace_id:
            metadata[UPLOAD_METADATA_KEYS["trace_id"]] = trace_id

        logger.debug(
            "S3: Starting actual upload",
            extra={
                "metadata": metadata,
                "file": file,
                "document_upload_bucket_name": bucket_name,
                "unique_file_name": unique_file_name,
            },
        )

        s3_service.upload_file(bucket_name, object_key, file, content_type, metadata)
        logger.info("=== S3 UPLOAD SUCCESS ===")

    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
        logger.info(f"=== S3 UPLOAD FAILED: {e} ===")
        raise HTTPException(
            status_code=500,
            detail="Document upload failed",
        ) from e


async def get_v1_document_processing_results(job_id: str, timeout: int) -> JobStatusResponse:
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
                return JobStatusResponse(**json.loads(job_status.v1_response_json))

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
        result = classify_as_failed(
            object_key=object_key,
            error_message="Processing timeout",
            data=ClassificationData(
                additional_info=f"Processing did not complete within {timeout} seconds"
            ),
        )

        return JobStatusResponse(**result)
    else:
        # fallback if we never got a record
        return JobStatusResponse(
            job_id=job_id,
            job_status="failed",
            message=f"Processing timeout after {timeout} seconds",
        )


# protected endpoints (require authorization)
@app.post("/v1/documents", dependencies=[Depends(verify_api_key)])
async def create_document(
    request: Request,
    response: Response,
    file: UploadFile,
    category: Annotated[
        DocumentCategory | None, Form(description="Type of document being uploaded")
    ] = None,
    trace_id: Annotated[str | None, Header(alias="X-Trace-ID")] = None,
    wait: bool = False,  # async by default
    timeout: int = 180,  # accounts for ECS cold starts and BDA processing time
) -> UploadAsyncResponse | JobStatusResponse:
    """Upload a document for processing.

    Args:
        wait: If true, waits for processing to complete before returning results.
              If false (default), returns immediately with job_id for async polling.
        timeout: Maximum seconds to wait when wait=true (default: 120)
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    if not trace_id:
        trace_id = str(uuid.uuid4())

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

    logger.info(
        f"Processing {file.filename}; category: {category}; content-type: {actual_content_type}"
    )

    file.file.seek(0)
    file_extension = file.filename.split(".")[-1]
    file_name = file.filename.split(".")[0]
    unique_file_name = f"{file_name}-{uuid.uuid4()}.{file_extension}"
    job_id = str(uuid.uuid4())

    await upload_document_for_processing(
        file=file.file,
        original_file_name=file.filename,
        unique_file_name=unique_file_name,
        content_type=actual_content_type,
        user_provided_document_category=category,
        job_id=job_id,
        trace_id=trace_id,
    )

    response.headers["X-Trace-ID"] = trace_id
    if not wait:
        return UploadAsyncResponse(
            job_id=job_id,
            job_status=ProcessStatus.NOT_STARTED.value,
            message="Document uploaded successfully",
        )
    else:
        return await get_v1_document_processing_results(job_id, timeout)


@app.get("/v1/documents/{job_id}", dependencies=[Depends(verify_api_key)])
async def get_document_results(
    job_id: str, include_extracted_data: bool = False
) -> JobStatusResponse:
    """Get processing results by job ID."""
    try:
        job_status = _get_job_status(job_id)

        if not job_status.ddb_record:
            raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found")

        if not job_status.v1_response_json:
            return JobStatusResponse(
                job_id=job_id,
                job_status=job_status.process_status or "processing",
                message="Processing in progress",
            )

        # processing complete
        if include_extracted_data:
            # rebuild response with extracted data
            from documentai_api.utils.response_builder import build_v1_api_response

            if not job_status.object_key or not job_status.process_status:
                raise HTTPException(status_code=500, detail=f"Incomplete record for job {job_id}")

            return JobStatusResponse(
                **build_v1_api_response(
                    object_key=job_status.object_key,
                    job_status=job_status.process_status,
                    include_extracted_data=True,
                )
            )
        else:
            # return cached response without extracted data
            return JobStatusResponse(**json.loads(job_status.v1_response_json))

    except HTTPException:
        raise
    except Exception as e:
        msg = f"Error retrieving results for job {job_id}: {e}"
        logger.error(msg)
        raise HTTPException(status_code=500, detail="Failed to retrieve results") from e


@app.get("/v1/schemas", dependencies=[Depends(verify_api_key)])
async def list_schemas() -> SchemaListResponse:
    """List all supported document types."""
    schemas = get_all_schemas()
    return SchemaListResponse(schemas=list(schemas.keys()))


@app.get("/v1/schemas/{document_type}", dependencies=[Depends(verify_api_key)])
async def get_schema(document_type: str) -> SchemaDetailResponse:
    """Get field schema for a specific document type."""
    schema = get_document_schema(document_type)

    if not schema:
        raise HTTPException(
            status_code=404, detail=f"Schema not found for document type: {document_type}"
        )

    return SchemaDetailResponse(**schema)
