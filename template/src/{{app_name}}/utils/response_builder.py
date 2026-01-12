"""Utility to build standardized API responses for document processing results."""
from datetime import datetime, timezone
from typing import Any
from {{app_name}}.utils.models import ClassificationData, V1ApiResponse
from {{app_name}}.schemas.document_metadata import DocumentMetadata
from {{app_name}}.config.settings import (
    ProcessStatus,
    PROCESSING_STATUS_NOT_SUPPORTED,
    PROCESSING_STATUS_PENDING_EXTRACTION,
    PROCESSING_STATUS_SUCCESS
)
from {{app_name}}.utils.response_codes import ResponseCodes

def _to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase"""
    components = snake_str.split('_')
    return components[0].lower() + ''.join(word.capitalize() for word in components[1:])


def _extract_field_values(data: ClassificationData) -> dict[str, Any]:
    """Extract field data for API response """
    if not data or not data.field_confidence_scores:
        return {}
    
    fields = {}
    for field_data in data.field_confidence_scores:
        for field_name, confidence in field_data.items():
            camel_field = _to_camel_case(field_name)
            fields[camel_field] = {
                "confidence": round(confidence, 2),
                "value": "<redacted>"
            }
    
    return fields

def _get_real_field_values_from_s3(ddb_record: dict) -> dict:
    """Get real field values from S3, keeping confidence from stored response"""
    # get bda extracted values from s3
    pass

def get_v1_api_response(
    object_key: str,
    user_provided_document_category: str,
    response_code: str,
    document_type: str | None,
) -> V1ApiResponse:
    """
    Get API response object for external consumers.
    
    Args:
        object_key: S3 file key
        document_type: Detected document type
        response_code: Processing result code
        user_provided_document_category: User-specified category
        
    Returns:
        V1ApiResponse: Response object for API endpoints
    """

    return V1ApiResponse(
        validation_passed=ResponseCodes.is_success_response_code(response_code),
        document_category=user_provided_document_category,
        document_type=document_type,
        response_code=response_code,
        response_message=ResponseCodes.get_message(response_code),
    )

def build_v1_api_response(
    status: str, 
    data: ClassificationData | None = None, 
    error_message: str  | None = None,
) -> dict[str, Any]:
    """
    Build API response dict for DDB storage.
    
    Args:
        status: Processing status
        data: Classification data with field results
        error_message: Error details if failed
        
    Returns:
        dict: Response data for DDB JSON storage
    """
    
    base_response = {
        "status": status,
        "processedAt": datetime.now(timezone.utc).isoformat()
    }
    
    # success response with full results
    if status in PROCESSING_STATUS_SUCCESS:
        base_response["status"] = "completed"
    
        if status == DocumentMetadata.ProcessStatus.SUCCESS:
            base_response["message"] = "Document processed successfully"
        elif status == DocumentMetadata.ProcessStatus.NO_CUSTOM_BLUEPRINT_MATCHED:
            base_response["message"] = "Document processed but no matching template found"

        if data:
            base_response.update({
                "documentType": data.document_type,
                "fields": _extract_field_values(data)
            })
    
    # error responses
    elif status == DocumentMetadata.ProcessStatus.FAILED:
        base_response.update({
            "status": "failed",
            "error": error_message or "Processing failed",
            "additionalInfo": data.additional_info if data else None
        })
    
    elif status in PROCESSING_STATUS_NOT_SUPPORTED:
        base_response.update({
            "status": "not_supported",
            "message": "Document type not supported",
            "additionalInfo": data.additional_info if data else None
        })
    
    else:
        base_response.update({
            "status": "processing",
            "message": "Document processing in progress"
        })
    
    # Remove None values for cleaner response
    return {k: v for k, v in base_response.items() if v is not None}

__all__ = ["get_v1_api_response", "build_v1_api_response"]