"""Utility to build standardized API responses for document processing results."""

import json
from typing import Any

from documentai_api.config.constants import (
    PROCESSING_STATUS_NOT_SUPPORTED,
    PROCESSING_STATUSES_SUCCESSFUL,
    DocumentCategory,
    ProcessStatus,
)
from documentai_api.logging import get_logger
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services.bda import get_bda_result_json
from documentai_api.utils.bda import extract_field_values_from_bda_results
from documentai_api.utils.models import ClassificationData, InternalApiResponse
from documentai_api.utils.response_codes import ResponseCodes
from documentai_api.utils.strings import snake_to_camel

logger = get_logger(__name__)


# TODO: Refactor to improve testability - consider making public along with
# restructuring to reduce mocking in tests
def _extract_field_values(
    ddb_record: dict[str, Any], include_extracted_data: bool
) -> dict[str, Any]:
    """Extract field data for API response."""
    if not ddb_record:
        return {}

    # get confidence scores and extracted values if requested
    if include_extracted_data:
        s3_uri = ddb_record.get(DocumentMetadata.BDA_OUTPUT_S3_URI)

        if not s3_uri:
            return {}

        bda_results = get_bda_result_json(s3_uri)

        if not bda_results:
            return {}

        metadata, field_values = extract_field_values_from_bda_results(bda_results)
        field_confidence_map_list = metadata.field_confidence_map_list
    else:
        field_confidence_map_list = json.loads(
            ddb_record.get(DocumentMetadata.FIELD_CONFIDENCE_SCORES, "[]")
        )
        field_values = {}

    # build response
    fields = {}
    for field_item in field_confidence_map_list:
        for field_name, confidence in field_item.items():
            camel_field = snake_to_camel(field_name)
            fields[camel_field] = {
                "confidence": round(confidence, 2),
                "value": field_values.get(field_name) if include_extracted_data else "<redacted>",
            }

    return fields


def get_internal_api_response(
    object_key: str,
    response_code: str,
    matched_document_class: str | None,
    user_provided_document_category: str | None = None,
) -> InternalApiResponse:
    """Get API response object for internal use.

    Args:
        object_key: S3 file key
        response_code: Processing result code
        document_type: Detected document type
        user_provided_document_category: Document category provided by user at upload time
    Returns:
        InternalApiResponse: Response object for API endpoints
    """
    # import here to avoid circular dependency
    if not user_provided_document_category:
        from documentai_api.utils.ddb import get_user_provided_document_category

        user_provided_document_category = get_user_provided_document_category(object_key)

    return InternalApiResponse(
        validation_passed=ResponseCodes.is_success_response_code(response_code),
        document_category=DocumentCategory(user_provided_document_category)
        if user_provided_document_category
        else None,
        matched_document_class=matched_document_class,
        response_code=response_code,
        response_message=ResponseCodes.get_message(response_code),
    )


def build_v1_api_response(
    object_key: str,
    status: str,
    data: ClassificationData | None = None,
    error_message: str | None = None,
    include_extracted_data: bool = False,
) -> dict[str, Any]:
    """Build API response dict for DDB storage.

    Args:
        status: Processing status
        data: Classification data with field results
        error_message: Error details if failed

    Returns:
        dict: Response data for DDB JSON storage
    """
    status = status.value if isinstance(status, ProcessStatus) else status
    from documentai_api.utils.ddb import get_ddb_record

    ddb_record = get_ddb_record(object_key)
    job_id = ddb_record.get(DocumentMetadata.JOB_ID)
    matched_document_class = ddb_record.get(DocumentMetadata.BDA_MATCHED_DOCUMENT_CLASS)
    total_time = ddb_record.get(DocumentMetadata.TOTAL_PROCESSING_TIME_SECONDS)
    created_at = ddb_record.get(DocumentMetadata.CREATED_AT)
    completed_at = ddb_record.get(DocumentMetadata.BDA_COMPLETED_AT)

    base_response = {"jobId": job_id, "status": status, "createdAt": created_at}

    if completed_at:
        base_response["completedAt"] = completed_at

    if total_time:
        base_response["totalProcessingTimeSeconds"] = float(total_time)

    if matched_document_class:
        base_response["matchedDocumentClass"] = matched_document_class

    # success response with full results
    if status in PROCESSING_STATUSES_SUCCESSFUL:
        base_response["status"] = "completed"

        if status == ProcessStatus.SUCCESS.value:
            base_response["message"] = "Document processed successfully"
        elif status == ProcessStatus.NO_CUSTOM_BLUEPRINT_MATCHED.value:
            base_response["message"] = "Document processed but no matching template found"

        base_response.update({"fields": _extract_field_values(ddb_record, include_extracted_data)})

    # error responses
    elif status == ProcessStatus.FAILED.value:
        base_response.update(
            {
                "status": "failed",
                "error": error_message or "Processing failed",
                "additionalInfo": data.additional_info if data else None,
            }
        )

    elif status == ProcessStatus.NO_DOCUMENT_DETECTED.value:
        base_response.update(
            {
                "status": "not_supported",
                "message": "Unable to extract meaningful document content",
                "additionalInfo": data.additional_info if data else None,
            }
        )

    elif status in PROCESSING_STATUS_NOT_SUPPORTED:
        base_response.update(
            {
                "status": "not_supported",
                "message": "Document type not supported",
                "additionalInfo": data.additional_info if data else None,
            }
        )

    else:
        base_response.update({"status": "processing", "message": "Document processing in progress"})

    # Remove None values for cleaner response
    return {k: v for k, v in base_response.items() if v is not None}


__all__ = ["build_v1_api_response", "get_internal_api_response"]
