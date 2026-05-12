import json
import random
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from documentai_api.config.constants import (
    PROCESSING_STATUS_COMPLETED,
    PROCESSING_STATUS_PENDING_EXTRACTION,
    ConfigDefaults,
    DocumentCategory,
    ProcessStatus,
)
from documentai_api.logging import get_logger
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import ddb as ddb_service
from documentai_api.services import s3 as s3_service
from documentai_api.utils import env
from documentai_api.utils import s3 as s3_utils
from documentai_api.utils.document_detector import (
    DocumentDetector,
    QualityMetricsNormalized,
    QualityMetricsRaw,
)
from documentai_api.utils.env import get_required_env
from documentai_api.utils.models import (
    ClassificationData,
    FieldMetrics,
    InternalApiResponse,
    ProcessingTimes,
)
from documentai_api.utils.response_builder import build_v1_api_response, get_internal_api_response
from documentai_api.utils.response_codes import ResponseCodes

logger = get_logger(__name__)


def extract_region_from_bda_arn(bda_invocation_arn: str) -> str | None:
    """Extract AWS region from BDA invocation ARN."""
    try:
        # arn format: arn:aws:bedrock-data-automation:us-east-1:account:job/job-id
        parts = bda_invocation_arn.split(":")
        if len(parts) >= 4:
            return parts[3]  # Region is the 4th part
        return None
    except Exception as e:
        logger.error(f"Failed to extract region from ARN {bda_invocation_arn}: {e}")
        return None


def get_elapsed_time_seconds(start_time: datetime, end_time: datetime) -> Decimal:
    """Calculate elapsed time in seconds with 2 decimal precision."""
    return Decimal(str(round((end_time - start_time).total_seconds(), 2)))


def calculate_bda_processing_times(object_key: str, completion_time: datetime) -> ProcessingTimes:
    """Calculate BDA processing timing metrics.

    Returns dict with timing data to add to DDB update, or empty dict if calculation fails.
    """
    ddb_record = get_ddb_record(object_key)
    if ddb_record is None:
        return ProcessingTimes()

    created_at_str = ddb_record.get(DocumentMetadata.CREATED_AT)
    bda_started_at_str = ddb_record.get(DocumentMetadata.BDA_STARTED_AT)

    timing_data = ProcessingTimes()

    if created_at_str:
        created_at = datetime.fromisoformat(created_at_str)
        total_processing_time_seconds = get_elapsed_time_seconds(created_at, completion_time)
        timing_data.total_processing_time_seconds = total_processing_time_seconds
        logger.info(f"Total processing time: {total_processing_time_seconds:.2f} seconds")

    if bda_started_at_str:
        bda_started_at = datetime.fromisoformat(bda_started_at_str)
        bda_processing_time_seconds = get_elapsed_time_seconds(bda_started_at, completion_time)
        timing_data.bda_processing_time_seconds = bda_processing_time_seconds
        logger.info(f"BDA processing time: {bda_processing_time_seconds:.2f} seconds")

    return timing_data


def _calculate_wait_time(object_key: str) -> Decimal | None:
    """Calculate BDA wait time from file creation to BDA start."""
    ddb_record = get_ddb_record(object_key)
    if ddb_record is None:
        return None

    created_at_str = ddb_record.get(DocumentMetadata.CREATED_AT)

    if not created_at_str:
        return None

    created_at = datetime.fromisoformat(created_at_str)
    return get_elapsed_time_seconds(created_at, datetime.now(UTC))


def _calculate_field_metrics(data: ClassificationData) -> FieldMetrics:
    """Calculate field count metrics from classification data."""
    if not data.field_confidence_scores:
        return FieldMetrics(0, 0, None)

    field_count = len(data.field_confidence_scores)
    empty_fields = set(data.field_empty_list or [])

    # Count non-empty fields and sum their confidence scores
    non_empty_count = 0
    confidence_sum = 0

    for field_data in data.field_confidence_scores:
        field_name = next(iter(field_data.keys()))
        confidence = next(iter(field_data.values()))

        if field_name not in empty_fields:
            non_empty_count += 1
            confidence_sum += confidence

    avg_confidence = confidence_sum / non_empty_count if non_empty_count > 0 else None

    return FieldMetrics(field_count, non_empty_count, avg_confidence)


def _build_completion_timing(
    object_key: str, bda_output_s3_uri: str | None
) -> tuple[list[str], dict[str, Any]]:
    """Build completion timing updates."""
    updates: list[str] = []
    values: dict[str, Any] = {}

    ddb_record = get_ddb_record(object_key)
    # record doesn't exist yet (eg. pre-ddb insert failure), skip bda timing
    if ddb_record is None:
        return updates, values

    if ddb_record.get(DocumentMetadata.BDA_STARTED_AT):
        completed_time = datetime.now(UTC)

        # use S3 LastModified timestamp if available
        if bda_output_s3_uri:
            try:
                bucket, key = s3_utils.parse_s3_uri(bda_output_s3_uri)
                completed_time = s3_service.get_last_modified_at(bucket, key)
                logger.info(f"Using S3 LastModified for bdaCompletedAt: {completed_time}")
            except Exception as e:
                logger.warning(
                    f"Failed to get S3 timestamp for bdaCompletedAt, using current time: {e}"
                )

        updates.append(f"{DocumentMetadata.BDA_COMPLETED_AT} = :bdaCompletedAt")
        values[":bdaCompletedAt"] = completed_time.isoformat()

        updates.append(f"{DocumentMetadata.PROCESSED_DATE} = :processedDate")
        values[":processedDate"] = completed_time.strftime("%Y-%m-%d")

        timing_data = calculate_bda_processing_times(object_key, completed_time)

        if timing_data.total_processing_time_seconds:
            updates.append(
                f"{DocumentMetadata.TOTAL_PROCESSING_TIME_SECONDS} = :totalProcessingTime"
            )
            values[":totalProcessingTime"] = timing_data.total_processing_time_seconds

        if timing_data.bda_processing_time_seconds:
            updates.append(f"{DocumentMetadata.BDA_PROCESSING_TIME_SECONDS} = :bdaProcessingTime")
            values[":bdaProcessingTime"] = timing_data.bda_processing_time_seconds

    return updates, values


def _build_timing_updates(
    object_key: str, status: str, bda_output_s3_uri: str | None
) -> tuple[str, dict[str, Any]]:
    """Handle all timing-related updates for different statuses."""
    status = status.value if isinstance(status, ProcessStatus) else status

    updates = []
    values: dict[str, Any] = {}

    if status == ProcessStatus.STARTED:
        updates.append(f"{DocumentMetadata.BDA_STARTED_AT} = :bdaStartedAt")
        values[":bdaStartedAt"] = datetime.now(UTC).isoformat()

        try:
            wait_time = _calculate_wait_time(object_key)
            updates.append(f"{DocumentMetadata.BDA_WAIT_TIME_SECONDS} = :bdaWaitTimeSeconds")
            values[":bdaWaitTimeSeconds"] = wait_time
        except Exception as e:
            logger.error(f"Failed to calculate bda wait time for {object_key}: {e}")

    elif status in PROCESSING_STATUS_COMPLETED:
        completion_updates, completion_values = _build_completion_timing(
            object_key, bda_output_s3_uri
        )
        updates.extend(completion_updates)
        values.update(completion_values)

    return ", ".join(updates), values


def _build_update_expression(
    status: str,
    data: ClassificationData | None,
    internal_api_response: InternalApiResponse | None,
    v1_api_response: str | None,
    bda_invocation_arn: str | None = None,
    error_message: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build DynamoDB update expression and values."""
    updates = [
        f"{DocumentMetadata.PROCESS_STATUS} = :processStatus",
        f"{DocumentMetadata.UPDATED_AT} = :updatedAt",
    ]

    values: dict[str, Any] = {":processStatus": status, ":updatedAt": datetime.now(UTC).isoformat()}

    if data:
        metrics = _calculate_field_metrics(data)

        field_mappings = {
            DocumentMetadata.BDA_OUTPUT_S3_URI: data.bda_output_s3_uri,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_NAME: data.matched_blueprint_name,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_CONFIDENCE: data.matched_blueprint_confidence,
            DocumentMetadata.FIELD_CONFIDENCE_SCORES: data.field_confidence_scores,
            DocumentMetadata.ADDITIONAL_INFO: data.additional_info,
            DocumentMetadata.BDA_MATCHED_DOCUMENT_CLASS: data.matched_document_class,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_FIELD_EMPTY_LIST: data.field_empty_list,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_FIELD_BELOW_THRESHOLD_LIST: data.field_below_threshold_list,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_FIELD_COUNT: metrics.field_count,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_FIELD_COUNT_NOT_EMPTY: metrics.field_count_not_empty,
            DocumentMetadata.BDA_MATCHED_BLUEPRINT_FIELD_NOT_EMPTY_AVG_CONFIDENCE: metrics.field_not_empty_avg_confidence,
        }

        for ddb_field, value in field_mappings.items():
            if value is not None:
                param_key = f":{ddb_field.lower().replace('_', '')}"
                updates.append(f"{ddb_field} = {param_key}")

                if isinstance(value, (list, dict)):
                    values[param_key] = json.dumps(value)
                elif isinstance(value, float):
                    values[param_key] = Decimal(str(value))
                else:
                    values[param_key] = value

    if internal_api_response:
        updates.append(f"{DocumentMetadata.RESPONSE_JSON} = :responseJson")
        values[":responseJson"] = json.dumps(internal_api_response.__dict__)

        updates.append(f"{DocumentMetadata.RESPONSE_CODE} = :responseCode")
        values[":responseCode"] = internal_api_response.response_code

    if v1_api_response:
        updates.append(f"{DocumentMetadata.V1_API_RESPONSE_JSON} = :v1ResponseJson")
        values[":v1ResponseJson"] = json.dumps(v1_api_response)

    if bda_invocation_arn:
        updates.append(f"{DocumentMetadata.BDA_INVOCATION_ARN} = :bdaInvocationArn")
        values[":bdaInvocationArn"] = bda_invocation_arn

        bda_region = (
            extract_region_from_bda_arn(bda_invocation_arn)
            or ConfigDefaults.BDA_REGION_NOT_AVAILABLE.value
        )
        updates.append(f"{DocumentMetadata.BDA_REGION_USED} = :bdaRegion")
        values[":bdaRegion"] = bda_region

    if error_message:
        updates.append(f"{DocumentMetadata.ERROR_MESSAGE} = :errorMessage")
        values[":errorMessage"] = error_message

    return "SET " + ", ".join(updates), values


def _execute_ddb_update(
    object_key: str, update_expression: str, expression_values: dict[str, Any]
) -> None:
    """Execute the DynamoDB update."""
    table_name = get_required_env(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)
    key = {"fileName": object_key}

    ddb_service.update_item(table_name, key, update_expression, expression_values)


def get_user_provided_document_category(object_key: str) -> DocumentCategory | None:
    """Get user specified document type for a file.

    This should always succeed - the user document type is set when the file
    is first processed. If this fails, we have a data pipeline problem.
    """
    ddb_record = get_ddb_record(object_key)
    if ddb_record is None:
        return None

    user_provided_document_category = ddb_record.get(
        DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY
    )

    if not user_provided_document_category:
        logger.warning(f"User specified document type not found for file: {object_key}")

    return (
        DocumentCategory(user_provided_document_category)
        if user_provided_document_category
        else None
    )


def get_ddb_record(object_key: str) -> dict[str, Any] | None:
    """Get DDB record by file name."""
    table_name = get_required_env(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)
    key = {"fileName": object_key}
    item = ddb_service.get_item(table_name, key)

    if not item:
        return None

    return item


def get_ddb_by_job_id(job_id: str) -> dict[str, Any] | None:
    """Get document metadata record by job ID."""
    table_name = get_required_env(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)
    index_name = get_required_env(env.DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME)

    items = ddb_service.query_by_key(table_name, index_name, "jobId", job_id)
    return items[0] if items else None


def update_ddb(
    object_key: str,
    status: str,
    internal_api_response: InternalApiResponse | None = None,
    data: ClassificationData | None = None,
    bda_invocation_arn: str | None = None,
    error_message: str | None = None,
) -> None:
    """Update DynamoDB processing status for a file."""
    try:
        # build base update expression (without v1_response)
        update_expr, expr_values = _build_update_expression(
            status=status,
            data=data,
            internal_api_response=internal_api_response,
            v1_api_response=None,  # built after ddb update
            bda_invocation_arn=bda_invocation_arn,
            error_message=error_message,
        )

        # add timing updates
        timing_updates, timing_values = _build_timing_updates(
            object_key, status, bda_output_s3_uri=data.bda_output_s3_uri if data else None
        )
        if timing_updates:
            update_expr += f", {timing_updates}"
            expr_values.update(timing_values)

        _execute_ddb_update(object_key, update_expr, expr_values)

        # build v1 response after ddb has been updated
        v1_response = build_v1_api_response(object_key, status, data, error_message=error_message)

        # update ddb again with v1_response
        update_expr = f"SET {DocumentMetadata.V1_API_RESPONSE_JSON} = :v1ResponseJson"
        expr_values = {":v1ResponseJson": json.dumps(v1_response)}
        _execute_ddb_update(object_key, update_expr, expr_values)

    except Exception as e:
        logger.error(f"Failed to update DDB status: {e}")
        raise


def insert_ddb(
    object_key: str,
    original_file_name: str,
    user_provided_document_category: str | None = None,
    process_status: str | None = None,
    internal_api_response: InternalApiResponse | None = None,
    file_size_bytes: int | None = None,
    content_type: str | None = None,
    pages_detected: int | None = None,
    job_id: str | None = None,
    trace_id: str | None = None,
    is_password_protected: bool | None = False,
    is_document_blurry: bool | None = False,
    document_profile_raw_metrics: QualityMetricsRaw | None = None,
    document_profile_normalized_metrics: QualityMetricsNormalized | None = None,
    overall_blur_score: float | None = None,
) -> None:
    try:
        table_name = get_required_env(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)

        item: dict[str, Any] = {
            DocumentMetadata.FILE_NAME: object_key,
            DocumentMetadata.ORIGINAL_FILE_NAME: original_file_name,
            DocumentMetadata.PROCESS_STATUS: process_status,
            DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY: (
                user_provided_document_category
                or ConfigDefaults.USER_DOCUMENT_TYPE_NOT_PROVIDED.value
            ),
            DocumentMetadata.CREATED_AT: datetime.now(UTC).isoformat(),
            DocumentMetadata.UPDATED_AT: datetime.now(UTC).isoformat(),
        }

        if file_size_bytes is not None:
            item[DocumentMetadata.FILE_SIZE_BYTES] = file_size_bytes
        if content_type:
            item[DocumentMetadata.CONTENT_TYPE] = content_type

        if pages_detected is not None:
            item[DocumentMetadata.PAGES_DETECTED] = pages_detected

        if internal_api_response:
            item[DocumentMetadata.RESPONSE_JSON] = json.dumps(internal_api_response.__dict__)

        if job_id:
            item[DocumentMetadata.JOB_ID] = job_id

        if trace_id:
            item[DocumentMetadata.TRACE_ID] = trace_id

        if is_password_protected is not None:
            item[DocumentMetadata.IS_PASSWORD_PROTECTED] = bool(is_password_protected)

        if is_document_blurry is not None:
            item[DocumentMetadata.IS_DOCUMENT_BLURRY] = bool(is_document_blurry)

        if document_profile_raw_metrics is not None:
            item[DocumentMetadata.DOCUMENT_METRICS_RAW] = json.dumps(
                document_profile_raw_metrics.to_json_dict()
            )

        if document_profile_normalized_metrics is not None:
            item[DocumentMetadata.DOCUMENT_METRICS_NORMALIZED] = json.dumps(
                document_profile_normalized_metrics.to_json_dict()
            )

        if overall_blur_score is not None:
            item[DocumentMetadata.OVERALL_BLUR_SCORE] = Decimal(str(overall_blur_score))

        ddb_service.put_item(table_name, item)

    except Exception as e:
        logger.error(f"Failed to create DDB record for {object_key}: {e}")
        raise


def insert_minimal_ddb_record(
    ddb_key: str,
    original_file_name: str,
    job_id: str,
    process_status: ProcessStatus = ProcessStatus.NOT_STARTED,
    user_provided_document_category: str | None = None,
    trace_id: str | None = None,
    content_type: str | None = None,
    file_size_bytes: int | None = None,
) -> None:
    """Create initial tracking record."""
    insert_ddb(
        object_key=ddb_key,
        original_file_name=original_file_name,
        user_provided_document_category=user_provided_document_category,
        process_status=process_status,
        file_size_bytes=file_size_bytes,
        content_type=content_type,
        job_id=job_id,
        trace_id=trace_id,
    )


def insert_initial_ddb_record(
    source_bucket_name: str,
    source_object_key: str,
    ddb_key: str,
    original_file_name: str,
    user_provided_document_category: str | None = None,
    job_id: str | None = None,
    trace_id: str | None = None,
) -> None:
    """Insert initial DDB record."""
    if not user_provided_document_category:
        logger.warning(f"Warning: user_provided_document_category is None/empty for {ddb_key}")
        user_provided_document_category = "unknown"

    content_type = s3_service.get_content_type(source_bucket_name, source_object_key)
    file_size_bytes = s3_service.get_file_size_bytes(source_bucket_name, source_object_key)
    file_bytes = s3_service.get_file_bytes(source_bucket_name, source_object_key)

    bda_percentage = 1.0  # TODO: fetch from SSM
    is_multipage_detection_enabled = False  # TODO: add SSM configuration
    response_code = ResponseCodes.SUCCESS
    internal_api_response: InternalApiResponse | None = None
    process_status = ProcessStatus.PENDING_GRAYSCALE_CONVERSION
    pages_detected = None

    document_detector = DocumentDetector()
    profile = document_detector.get_document_profile(file_bytes, source_object_key)
    pages_detected = profile.page_count
    is_password_protected = profile.is_password_protected
    is_document_blurry = profile.is_blurry
    document_profile_raw_metrics = profile.raw_metrics
    document_profile_normalized_metrics = profile.normalized_metrics
    overall_blur_score = profile.overall_blur_score

    if content_type == "image/bmp":
        process_status = ProcessStatus.NOT_IMPLEMENTED
        response_code = ResponseCodes.BITMAP_RECEIVED

    elif is_password_protected:
        process_status = ProcessStatus.PASSWORD_PROTECTED
        response_code = ResponseCodes.MISSING_FIELDS

    elif bda_percentage == 0.0 or not bda_percentage:
        process_status = ProcessStatus.NOT_IMPLEMENTED
        response_code = ResponseCodes.DOCUMENT_TYPE_NOT_IMPLEMENTED

    elif bda_percentage == 1.0 or random.random() <= bda_percentage:
        if is_document_blurry:
            process_status = ProcessStatus.BLURRY_DOCUMENT_DETECTED
            response_code = ResponseCodes.BLURRY_DOCUMENT_DETECTED

        else:
            if content_type in ["image/jpeg", "image/png", "image/bmp", "image/tiff"]:
                # image file - needs grayscale conversion first
                process_status = ProcessStatus.PENDING_GRAYSCALE_CONVERSION
            else:
                # non-image file - can go directly to BDA
                process_status = ProcessStatus.NOT_STARTED

            if is_multipage_detection_enabled and file_bytes:
                logger.info("=== Starting multi-page detection validation ===")

                try:
                    if profile.is_multipage:
                        logger.info(f"{ddb_key} is a multipage doc")
                        process_status = ProcessStatus.MULTIPAGE
                        response_code = ResponseCodes.MULTIPAGE_DOCUMENT

                    else:
                        logger.info(f"{ddb_key} is a single page doc")

                except Exception as e:
                    logger.info(f"=== Multipage detection failed: {e} ===")

            logger.info("=== Finished multi-page detection validation ===")

    else:
        process_status = ProcessStatus.NOT_SAMPLED
        response_code = ResponseCodes.SUCCESS

    # initial status does not qualify for bda processing
    # create the json response signaling the process is complete
    if process_status not in PROCESSING_STATUS_PENDING_EXTRACTION:
        internal_api_response = get_internal_api_response(
            object_key=ddb_key,
            response_code=response_code,
            matched_document_class=None,
            user_provided_document_category=user_provided_document_category,
        )

    insert_ddb(
        object_key=ddb_key,
        original_file_name=original_file_name,
        user_provided_document_category=user_provided_document_category,
        process_status=process_status,
        internal_api_response=internal_api_response,
        file_size_bytes=file_size_bytes,
        content_type=content_type,
        pages_detected=pages_detected,
        job_id=job_id,
        trace_id=trace_id,
        is_document_blurry=is_document_blurry,
        is_password_protected=is_password_protected,
        document_profile_raw_metrics=document_profile_raw_metrics,
        document_profile_normalized_metrics=document_profile_normalized_metrics,
        overall_blur_score=overall_blur_score,
    )

    # explicity remove file reference to free memory for the lambda
    del file_bytes


def set_bda_processing_status_started(object_key: str, bda_invocation_arn: str) -> None:
    """Mark file processing as started with BDA job ARN."""
    update_ddb(
        object_key=object_key,
        status=ProcessStatus.STARTED,
        internal_api_response=None,
        bda_invocation_arn=bda_invocation_arn,
    )


def set_bda_processing_status_not_started(object_key: str) -> None:
    update_ddb(
        object_key=object_key,
        status=ProcessStatus.NOT_STARTED,
        internal_api_response=None,
    )


def classify_as_success(
    object_key: str, response_code: str, data: ClassificationData
) -> dict[str, Any]:
    """Mark file processing as completed."""
    internal_api_response: InternalApiResponse = get_internal_api_response(
        object_key=object_key,
        response_code=response_code,
        matched_document_class=data.matched_document_class,
    )

    update_ddb(
        object_key=object_key,
        status=ProcessStatus.SUCCESS,
        internal_api_response=internal_api_response,
        data=data,
    )

    # convert dataclass to dict for JSON serialization
    return internal_api_response.__dict__


def classify_as_failed(
    object_key: str, error_message: str, data: ClassificationData
) -> dict[str, Any]:
    """Mark file processing as failed with error message."""
    internal_api_response: InternalApiResponse = get_internal_api_response(
        object_key=object_key,
        response_code=ResponseCodes.INTERNAL_PROCESSING_ERROR,
        matched_document_class=None,
    )

    update_ddb(
        object_key=object_key,
        status=ProcessStatus.FAILED,
        internal_api_response=internal_api_response,
        error_message=error_message,
        data=data,
    )

    # convert dataclass to dict for JSON serialization
    return internal_api_response.__dict__


def classify_as_not_implemented(object_key: str, data: ClassificationData) -> dict[str, Any]:
    """Mark file processing as not implemented."""
    internal_api_response: InternalApiResponse = get_internal_api_response(
        object_key=object_key,
        response_code=ResponseCodes.DOCUMENT_TYPE_NOT_IMPLEMENTED,
        matched_document_class=None,
    )

    update_ddb(
        object_key=object_key,
        status=ProcessStatus.SUCCESS,
        internal_api_response=internal_api_response,
        data=data,
    )

    # convert dataclass to dict for JSON serialization
    return internal_api_response.__dict__


def classify_as_no_document_detected(object_key: str, data: ClassificationData) -> dict[str, Any]:
    """Mark file processing as no document detected."""
    internal_api_response: InternalApiResponse = get_internal_api_response(
        object_key=object_key,
        response_code=ResponseCodes.NO_DOCUMENT_DETECTED,
        matched_document_class=None,
    )

    update_ddb(
        object_key=object_key,
        status=ProcessStatus.NO_DOCUMENT_DETECTED,
        internal_api_response=internal_api_response,
        data=data,
    )

    # convert dataclass to dict for JSON serialization
    return internal_api_response.__dict__


def classify_as_no_custom_blueprint_matched(
    object_key: str, data: ClassificationData
) -> dict[str, Any]:
    """Mark file processing as not implemented."""
    internal_api_response: InternalApiResponse = get_internal_api_response(
        object_key=object_key,
        response_code=ResponseCodes.DOCUMENT_TYPE_NOT_IMPLEMENTED,
        matched_document_class=None,
    )

    update_ddb(
        object_key=object_key,
        status=ProcessStatus.NO_CUSTOM_BLUEPRINT_MATCHED,
        internal_api_response=internal_api_response,
        data=data,
    )

    # convert dataclass to dict for JSON serialization
    return internal_api_response.__dict__
