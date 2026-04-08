import json
import os
import random
from datetime import UTC, datetime
from decimal import Decimal

import documentai_api.utils.documents as document_utils
from documentai_api.config.constants import (
    PROCESSING_STATUS_COMPLETED,
    PROCESSING_STATUS_PENDING_EXTRACTION,
    TEXTRACT_IDENTITY_DOCUMENT_TYPES,
    ConfigDefaults,
    ProcessStatus,
)
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import ddb as ddb_service
from documentai_api.services import s3 as s3_service
from documentai_api.utils import env
from documentai_api.utils import s3 as s3_utils
from documentai_api.utils.bedrock import preclassify_document_image
from documentai_api.utils.logger import get_logger
from documentai_api.utils.models import (
    ClassificationData,
    FieldMetrics,
    InternalApiResponse,
    ProcessingTimes,
    ExtractMethod
)
from documentai_api.utils.response_builder import build_v1_api_response, get_internal_api_response
from documentai_api.utils.response_codes import ResponseCodes
from documentai_api.utils.schemas import get_all_schemas
from documentai_api.services.textract import analyze_id
from documentai_api.utils.textract import extract_fields_from_analyze_id, get_id_type
from documentai_api.mappings import get_document_class, map_textract_to_bda_fields

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
    try:
        ddb_record = get_ddb_record(object_key)
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

    except Exception as e:
        logger.error(f"Failed to calculate completion timing: {e}")
        return ProcessingTimes()


def _calculate_wait_time(object_key: str) -> Decimal:
    """Calculate BDA wait time from file creation to BDA start."""
    ddb_record = get_ddb_record(object_key)
    created_at_str = ddb_record.get(DocumentMetadata.CREATED_AT)
    created_at = datetime.fromisoformat(created_at_str)
    current_time = datetime.now(UTC)
    return get_elapsed_time_seconds(created_at, current_time)


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


def _build_completion_timing(object_key: str, bda_output_s3_uri: str) -> tuple[list, dict]:
    """Build completion timing updates."""
    updates = []
    values = {}

    try:
        ddb_record = get_ddb_record(object_key)

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
                updates.append(
                    f"{DocumentMetadata.BDA_PROCESSING_TIME_SECONDS} = :bdaProcessingTime"
                )
                values[":bdaProcessingTime"] = timing_data.bda_processing_time_seconds
    except ValueError:
        # record doesn't exist yet (eg. pre-ddb insert failure), skip bda timing
        pass

    return updates, values


def _build_timing_updates(object_key: str, status: str, bda_output_s3_uri: str) -> tuple[str, dict]:
    """Handle all timing-related updates for different statuses."""
    status = status.value if isinstance(status, ProcessStatus) else status

    updates = []
    values = {}

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
    data: ClassificationData,
    internal_api_response: InternalApiResponse | None,
    v1_api_response: str | None,
    bda_invocation_arn: str | None = None,
    error_message: str | None = None,
) -> tuple[str, dict]:
    """Build DynamoDB update expression and values."""
    updates = [
        f"{DocumentMetadata.PROCESS_STATUS} = :processStatus",
        f"{DocumentMetadata.UPDATED_AT} = :updatedAt",
    ]

    values = {":processStatus": status, ":updatedAt": datetime.now(UTC).isoformat()}

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

        updates.append(f"{DocumentMetadata.EXTRACT_METHOD} = :extractMethod")
        values[":extractMethod"] = ExtractMethod.BDA

    if error_message:
        updates.append(f"{DocumentMetadata.ERROR_MESSAGE} = :errorMessage")
        values[":errorMessage"] = error_message

    return "SET " + ", ".join(updates), values


def _execute_ddb_update(object_key: str, update_expression: str, expression_values: dict):
    """Execute the DynamoDB update."""
    table_name = os.getenv(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)
    key = {"fileName": object_key}

    ddb_service.update_item(table_name, key, update_expression, expression_values)


def get_user_provided_document_category(object_key: str) -> str:
    """Get user specified document type for a file.

    This should always succeed - the user document type is set when the file
    is first processed. If this fails, we have a data pipeline problem.
    """
    ddb_record = get_ddb_record(object_key)
    user_provided_document_category = ddb_record.get(
        DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY
    )

    if not user_provided_document_category:
        logger.warning(f"User specified document type not found for file: {object_key}")

    return user_provided_document_category


def get_ddb_record(object_key: str) -> dict:
    """Get DDB record by file name. Raises ValueError if not found."""
    try:
        table_name = os.getenv(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)
        key = {"fileName": object_key}
        item = ddb_service.get_item(table_name, key)

        if not item:
            raise ValueError(f"DDB record not found for file: {object_key}")

        return item
    except Exception as e:
        logger.error(f"Failed to get DDB record for {object_key}: {e}")
        raise


def get_ddb_by_job_id(job_id: str) -> dict | None:
    """Get document metadata record by job ID."""
    table_name = os.getenv(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)
    index_name = os.getenv(env.DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME)

    if not index_name:
        raise ValueError(
            f"{env.DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME} environment variable not set"
        )

    items = ddb_service.query_by_key(table_name, index_name, "jobId", job_id)
    return items[0] if items else None


def update_ddb(
    object_key: str,
    status: str,
    internal_api_response: InternalApiResponse,
    data: ClassificationData | None = None,
    bda_invocation_arn: str | None = None,
    error_message: str | None = None,
):
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
    pre_classification_document_type: str | None = None,
    pre_classification_confidence: float | None = None,
):
    try:
        table_name = os.getenv(env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME)

        item = {
            DocumentMetadata.FILE_NAME: object_key,
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

        if pre_classification_document_type is not None:
            item[DocumentMetadata.PRE_CLASSIFICATION_DOCUMENT_TYPE] = (
                pre_classification_document_type
            )

        if pre_classification_confidence is not None:
            item[DocumentMetadata.PRE_CLASSIFICATION_CONFIDENCE] = Decimal(
                str(pre_classification_confidence)
            )

        ddb_service.put_item(table_name, item)

    except Exception as e:
        logger.error(f"Failed to create DDB record for {object_key}: {e}")
        raise


def insert_initial_ddb_record(
    source_bucket_name: str,
    source_object_key: str,
    ddb_key: str,
    user_provided_document_category: str,
    job_id: str | None = None,
    trace_id: str | None = None,
):
    """Insert initial DDB record."""
    if not user_provided_document_category:
        logger.warning(f"Warning: user_provided_document_category is None/empty for {ddb_key}")
        user_provided_document_category = "unknown"

    content_type = s3_service.get_content_type(source_bucket_name, source_object_key)
    file_size_bytes = s3_service.get_file_size_bytes(source_bucket_name, source_object_key)
    file_bytes = s3_service.get_file_bytes(source_bucket_name, source_object_key)

    bda_percentage = 1.0  # TODO: fetch from SSM
    response_code = ResponseCodes.SUCCESS
    internal_api_response = None
    process_status = ProcessStatus.PENDING_GRAYSCALE_CONVERSION
    pages_detected = None

    pages_detected = document_utils.get_page_count(file_bytes)
    is_password_protected = document_utils.is_password_protected(file_bytes)
    is_document_blurry = False
    pre_classification_document_type = None
    pre_classification_confidence = None
    is_textract_result = False
    extract_started_at = None
    extract_completed_at = None
    matched_document_class = None

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
        result = preclassify_document_image(
            file_bytes, content_type, list(get_all_schemas().keys())
        )
        pre_classification_document_type = result.document_type
        pre_classification_confidence = result.confidence

        if not result.is_document:
            # clearly not a document (cat, random photo, etc.)
            process_status = ProcessStatus.NO_DOCUMENT_DETECTED
            response_code = ResponseCodes.NO_DOCUMENT_DETECTED

        elif (
            result.document_type in ["not_a_document", "other_document"]
            and result.confidence < 0.85
        ):
            # it's a document but can't classify — likely blurry
            process_status = ProcessStatus.BLURRY_DOCUMENT_DETECTED
            response_code = ResponseCodes.BLURRY_DOCUMENT_DETECTED
            is_document_blurry = True

        elif result.document_count > 1:
            process_status = ProcessStatus.MULTIPLE_DOCUMENTS_ON_SINGLE_PAGE
            response_code = ResponseCodes.MULTIPLE_DOCUMENTS_ON_SINGLE_PAGE
        elif result.document_type in TEXTRACT_IDENTITY_DOCUMENT_TYPES:
            extract_started_at = datetime.now(UTC)
            textract_response = analyze_id(file_bytes)
            extract_completed_at = datetime.now(UTC)

            fields = extract_fields_from_analyze_id(textract_response)
            fields = map_textract_to_bda_fields(fields, result.document_type)
            id_type = get_id_type(textract_response)
            matched_document_class = get_document_class(id_type)
            
            # confidence only — no PII in DDB
            field_confidence_scores = [{name: data["confidence"]} for name, data in fields.items()]
            
            # write extracted values to S3
            output_bucket, output_prefix = s3_utils.parse_s3_uri(os.getenv(env.DOCUMENTAI_OUTPUT_LOCATION))
            textract_s3_key = f"{output_prefix}/textract/{ddb_key}.json"
            textract_s3_uri = f"s3://{output_bucket}/{textract_s3_key}"
            s3_service.put_object(
                output_bucket, textract_s3_key,
                json.dumps({"source": "textract", "fields": fields}).encode(),
                content_type="application/json",
            )

            logger.info(f"Textract identified document as {matched_document_class} with fields: {field_confidence_scores}")
            process_status = ProcessStatus.SUCCESS
            is_textract_result = True
        else:
            # document passed pre-classification, proceed to extraction
            if content_type in ["image/jpeg", "image/png", "image/bmp", "image/tiff"]:
                process_status = ProcessStatus.PENDING_GRAYSCALE_CONVERSION
            else:
                process_status = ProcessStatus.NOT_STARTED
    else:
        process_status = ProcessStatus.NOT_SAMPLED
        response_code = ResponseCodes.SUCCESS

    
    if process_status not in PROCESSING_STATUS_PENDING_EXTRACTION:
        internal_api_response: InternalApiResponse = get_internal_api_response(
            object_key=ddb_key,
            response_code=response_code,
            matched_document_class=None,
            user_provided_document_category=user_provided_document_category,
        )
    
    # initial status does not qualify for bda processing
    # create the json response signaling the process is complete
    insert_ddb(
        object_key=ddb_key,
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
        pre_classification_document_type=pre_classification_document_type,
        pre_classification_confidence=pre_classification_confidence,
    )

    if is_textract_result:
        
        data = ClassificationData(
            matched_document_class=matched_document_class,
            field_confidence_scores=field_confidence_scores,
            bda_output_s3_uri=textract_s3_uri,
        )

        internal_api_response = get_internal_api_response(
            object_key=ddb_key,
            response_code=response_code,
            matched_document_class=matched_document_class,
            user_provided_document_category=user_provided_document_category,
        )

        update_ddb(
            object_key=ddb_key,
            status=process_status,
            internal_api_response=internal_api_response,
            data=data,
        )

        # TODO: update timing logic - consider separate to separate method
        extract_time = get_elapsed_time_seconds(extract_started_at, extract_completed_at)
        _execute_ddb_update(
            ddb_key,
            f"SET {DocumentMetadata.EXTRACT_STARTED_AT} = :start, {DocumentMetadata.EXTRACT_COMPLETED_AT} = :end, {DocumentMetadata.EXTRACT_PROCESSING_TIME_SECONDS} = :time, {DocumentMetadata.EXTRACT_METHOD} = :method",
            {":start": extract_started_at.isoformat(), ":end": extract_completed_at.isoformat(), ":time": extract_time, ":method": ExtractMethod.TEXTRACT},
        )


    # explicity remove file reference to free memory for the lambda
    del file_bytes


def set_bda_processing_status_started(object_key: str, bda_invocation_arn: str):
    """Mark file processing as started with BDA job ARN."""
    update_ddb(
        object_key=object_key,
        status=ProcessStatus.STARTED,
        internal_api_response=None,
        bda_invocation_arn=bda_invocation_arn,
    )


def set_bda_processing_status_not_started(object_key: str):
    update_ddb(
        object_key=object_key,
        status=ProcessStatus.NOT_STARTED,
        internal_api_response=None,
    )


def classify_as_success(object_key: str, response_code: str, data: ClassificationData):
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


def classify_as_failed(object_key: str, error_message: str, data: ClassificationData):
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


def classify_as_not_implemented(object_key: str, data: ClassificationData):
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


def classify_as_no_document_detected(object_key: str, data: ClassificationData):
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


def classify_as_no_custom_blueprint_matched(object_key: str, data: ClassificationData):
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


def classify_as_multiple_documents_on_page(object_key: str, data: ClassificationData):
    """Mark file processing as multiple documents detected on single page."""
    internal_api_response: InternalApiResponse = get_internal_api_response(
        object_key=object_key,
        response_code=ResponseCodes.MULTIPLE_DOCUMENTS_ON_SINGLE_PAGE,
        matched_document_class=None,
    )

    update_ddb(
        object_key=object_key,
        status=ProcessStatus.MULTIPLE_DOCUMENTS_ON_SINGLE_PAGE,
        internal_api_response=internal_api_response,
        data=data,
    )

    # convert dataclass to dict for JSON serialization
    return internal_api_response.__dict__
