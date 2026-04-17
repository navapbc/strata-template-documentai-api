from datetime import UTC, datetime

import pytest

from documentai_api.config.constants import (
    PROCESSING_STATUSES_SUCCESSFUL,
    BdaResponseFields,
    ProcessStatus,
)
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.utils import response_builder as response_builder_util
from documentai_api.utils.models import ClassificationData, InternalApiResponse
from documentai_api.utils.response_codes import ResponseCodes


@pytest.mark.parametrize(
    ("response_code", "matched_document_class"),
    [
        (ResponseCodes.SUCCESS, "income"),
        (ResponseCodes.NO_DOCUMENT_DETECTED, "income"),
        (ResponseCodes.SUCCESS, None),
    ],
)
def test_get_internal_api_response(response_code, matched_document_class, ddb_doc_metadata_table):
    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-key",
        DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY: "income",
    }
    ddb_doc_metadata_table.put_item(Item=ddb_record)

    response = response_builder_util.get_internal_api_response(
        "test-key", response_code, matched_document_class
    )

    assert response == InternalApiResponse(
        validation_passed=ResponseCodes.is_success_response_code(response_code),
        document_category="income",
        matched_document_class=matched_document_class,
        response_code=response_code,
        response_message=ResponseCodes.get_message(response_code),
    )


@pytest.mark.parametrize(
    (
        "status",
        "error_message",
        "additional_info",
        "include_extracted_data",
        "expected_status",
        "expected_message",
        "expected_error",
    ),
    [
        (
            ProcessStatus.SUCCESS.value,
            None,
            None,
            False,
            "completed",
            "Document processed successfully",
            None,
        ),
        (
            ProcessStatus.SUCCESS.value,
            None,
            None,
            True,
            "completed",
            "Document processed successfully",
            None,
        ),
        (
            ProcessStatus.NO_CUSTOM_BLUEPRINT_MATCHED.value,
            None,
            None,
            False,
            "completed",
            "Document processed but no matching template found",
            None,
        ),
        (
            ProcessStatus.FAILED.value,
            "Test error",
            "Additional context",
            False,
            "failed",
            None,
            "Test error",
        ),
        (
            ProcessStatus.NO_DOCUMENT_DETECTED.value,
            None,
            "No content",
            False,
            "not_supported",
            "Unable to extract meaningful document content",
            None,
        ),
        (
            ProcessStatus.MULTIPAGE.value,
            None,
            "Unsupported type",
            False,
            "not_supported",
            "Document type not supported",
            None,
        ),
        (
            ProcessStatus.PASSWORD_PROTECTED.value,
            None,
            "Unsupported type",
            False,
            "not_supported",
            "Document type not supported",
            None,
        ),
        (
            ProcessStatus.STARTED.value,
            None,
            None,
            False,
            "processing",
            "Document processing in progress",
            None,
        ),
    ],
)
def test_build_v1_api_response(
    status: str,
    error_message: str | None,
    additional_info: str | None,
    include_extracted_data: bool,
    expected_status: str | None,
    expected_message: str | None,
    expected_error: str | None,
    s3_bucket,
    ddb_doc_metadata_table,
    mocker,
):
    import json

    year = datetime.now().year
    created_at = datetime(year, 1, 1, 12, 0, 0, tzinfo=UTC)
    bda_completed_at = datetime(year, 1, 1, 12, 0, 10, tzinfo=UTC)
    matched_document_class = "paystub"
    data = ClassificationData(
        matched_document_class=matched_document_class, additional_info=additional_info
    )

    bda_results = {
        BdaResponseFields.EXPLAINABILITY_INFO: [
            {
                "field_name_1": {"confidence": 0.95, "value": "value1"},
                "field_name_2": {"confidence": 0.85, "value": "value2"},
            }
        ]
    }
    bda_results_object = s3_bucket.put_object(Key="key.json", Body=json.dumps(bda_results))

    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-key",
        DocumentMetadata.JOB_ID: "test-job-id",
        DocumentMetadata.BDA_OUTPUT_S3_URI: f"s3://{bda_results_object.bucket_name}/{bda_results_object.key}",
        DocumentMetadata.BDA_MATCHED_DOCUMENT_CLASS: "paystub",
        DocumentMetadata.TOTAL_PROCESSING_TIME_SECONDS: 10,
        DocumentMetadata.BDA_COMPLETED_AT: bda_completed_at.isoformat(),
        DocumentMetadata.CREATED_AT: created_at.isoformat(),
        DocumentMetadata.FIELD_CONFIDENCE_SCORES: '[{"field_name_1": 0.95}, {"field_name_2": 0.85}]',
    }
    ddb_doc_metadata_table.put_item(Item=ddb_record)

    expected_fields_value = {
        "fieldName1": {
            "confidence": 0.95,
            "value": "value1" if include_extracted_data else "<redacted>",
        },
        "fieldName2": {
            "confidence": 0.85,
            "value": "value2" if include_extracted_data else "<redacted>",
        },
    }

    response = response_builder_util.build_v1_api_response(
        "test-key", status, data, error_message, include_extracted_data
    )

    expected_response = {
        "jobId": "test-job-id",
        "status": expected_status,
        "createdAt": created_at.isoformat(),
        "completedAt": bda_completed_at.isoformat(),
        "totalProcessingTimeSeconds": 10.0,
        "matchedDocumentClass": matched_document_class,
    }

    if expected_message:
        expected_response["message"] = expected_message

    if expected_error:
        expected_response["error"] = expected_error

    if additional_info:
        expected_response["additionalInfo"] = additional_info

    if status in PROCESSING_STATUSES_SUCCESSFUL:
        expected_response["fields"] = expected_fields_value

    assert response == expected_response


def test_build_v1_api_response_no_record(
    ddb_doc_metadata_table,
):
    with pytest.raises(ValueError, match="DDB record not found for file: test-does-not-exist"):
        response_builder_util.build_v1_api_response(
            "test-does-not-exist",
            ProcessStatus.SUCCESS,
            data=None,
            error_message=None,
            include_extracted_data=False,
        )


def test_build_v1_api_response_empty_record(
    ddb_doc_metadata_table,
):
    # Not really possible to have a truly empty dictionary returned, it needs to
    # at least have the primary key to be able to find at all/no error with "not
    # found"

    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-key",
    }
    ddb_doc_metadata_table.put_item(Item=ddb_record)

    response = response_builder_util.build_v1_api_response(
        "test-key",
        ProcessStatus.SUCCESS,
        data=None,
        error_message=None,
        include_extracted_data=False,
    )

    assert response == {
        "fields": dict(),
        "message": "Document processed successfully",
        "status": "completed",
    }
