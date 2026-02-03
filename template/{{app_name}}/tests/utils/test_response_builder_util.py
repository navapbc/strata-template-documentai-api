from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from documentai_api.config.constants import (
    PROCESSING_STATUSES_SUCCESSFUL,
    ProcessStatus,
)
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.utils import response_builder as response_builder_util
from documentai_api.utils.models import ClassificationData, InternalApiResponse
from documentai_api.utils.response_codes import ResponseCodes


@pytest.mark.parametrize(
    ("snake_case","expected_camel"),
    [
        ("user_provided_document_category", "userProvidedDocumentCategory"),
        ("job_id", "jobId"),
        ("trace_id", "traceId"),
        ("single", "single"),
        ("already_camelCase", "alreadyCamelcase"),
    ],
)
def test_to_camel_case(snake_case, expected_camel):
    assert response_builder_util._to_camel_case(snake_case) == expected_camel


def test_extract_field_values_empty_record():
    """Test with empty ddb_record."""
    result = response_builder_util._extract_field_values({}, False)
    assert result == {}


@pytest.mark.parametrize("include_extracted_data", [True, False])
def test_extract_field_values(include_extracted_data):
    with (
        patch("documentai_api.utils.response_builder.get_bda_result_json") as mock_get_bda,
        patch("documentai_api.utils.response_builder.extract_field_values_from_bda_results") as mock_extract_bda,
    ):
        from documentai_api.utils.bda import BdaFieldProcessingData

        mock_get_bda.return_value = {"fake": "bda_result"}

        # mock metadata with field_confidence_map_list
        mock_metadata = BdaFieldProcessingData(
            confidence_scores=[0.95, 0.85],
            empty_fields=[],
            field_confidence_map_list=[{"field_name_1": 0.95}, {"field_name_2": 0.85}],
        )
        mock_field_values = {"field_name_1": "value1", "field_name_2": "value2"}
        mock_extract_bda.return_value = (mock_metadata, mock_field_values)

        mock_ddb_record = {
            DocumentMetadata.BDA_OUTPUT_S3_URI: "s3://bucket/key",
            DocumentMetadata.FIELD_CONFIDENCE_SCORES: '[{"field_name_1": 0.95}, {"field_name_2": 0.85}]',
        }

        result = response_builder_util._extract_field_values(
            mock_ddb_record, include_extracted_data
        )

        assert "fieldName1" in result
        assert result["fieldName1"]["confidence"] == 0.95
        assert result["fieldName1"]["value"] == (
            "value1" if include_extracted_data else "<redacted>"
        )


@pytest.mark.parametrize(
    ("response_code","matched_document_class"),
    [
        (ResponseCodes.SUCCESS, "income"),
        (ResponseCodes.NO_DOCUMENT_DETECTED, "income"),
        (ResponseCodes.SUCCESS, None),
    ],
)
def test_get_internal_api_response(response_code, matched_document_class):

    with patch("documentai_api.utils.ddb.get_user_provided_document_category") as mock_get_category:
        mock_get_category.return_value = "income"

        response = response_builder_util.get_internal_api_response(
            "test-key", response_code, matched_document_class
        )

        assert response == InternalApiResponse(
            validation_passed=ResponseCodes.is_success_response_code(response_code),
            document_category=mock_get_category.return_value,
            matched_document_class=matched_document_class,
            response_code=response_code,
            response_message=ResponseCodes.get_message(response_code),
        )


@pytest.mark.parametrize(
    ("status","error_message","additional_info","include_extracted_data","expected_status","expected_message","expected_error"),
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
):
    year = datetime.now().year
    created_at = datetime(year, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    bda_completed_at = datetime(year, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
    matched_document_class = "paystub"
    data = ClassificationData(
        matched_document_class=matched_document_class, additional_info=additional_info
    )

    with (
        patch("documentai_api.utils.ddb.get_ddb_record") as mock_get_ddb_record,
        patch("documentai_api.utils.response_builder._extract_field_values") as mock_extract_field_values,
    ):

        mock_get_ddb_record.return_value = {
            DocumentMetadata.JOB_ID: "test-job-id",
            DocumentMetadata.BDA_MATCHED_DOCUMENT_CLASS: "paystub",
            DocumentMetadata.TOTAL_PROCESSING_TIME_SECONDS: 10,
            DocumentMetadata.BDA_COMPLETED_AT: bda_completed_at.isoformat(),
            DocumentMetadata.CREATED_AT: created_at.isoformat(),
        }

        mock_extract_field_values.return_value = {
            "field1": {
                "confidence": 0.95,
                "value": "data1" if include_extracted_data else "<redacted>",
            },
            "field2": {
                "confidence": 0.95,
                "value": "data2" if include_extracted_data else "<redacted>",
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
            expected_response["fields"] = mock_extract_field_values.return_value

        assert response == expected_response

        if status in PROCESSING_STATUSES_SUCCESSFUL:
            mock_extract_field_values.assert_called_once_with(
                mock_get_ddb_record.return_value, include_extracted_data
            )
        else:
            mock_extract_field_values.assert_not_called()
