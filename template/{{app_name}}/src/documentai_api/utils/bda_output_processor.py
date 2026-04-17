from dataclasses import dataclass, field
from typing import Any

from documentai_api.config.constants import BdaResponseFields, ConfigDefaults
from documentai_api.logging import get_logger
from documentai_api.services.bda import extract_bda_output_s3_uri, get_bda_result_json
from documentai_api.utils.bda import (
    BdaFieldProcessingData,
    extract_field_metadata_from_bda_results,
    get_text_from_standard_blueprint,
)
from documentai_api.utils.ddb import (
    classify_as_no_custom_blueprint_matched,
    classify_as_no_document_detected,
    classify_as_not_implemented,
    classify_as_success,
    get_user_provided_document_category,
)
from documentai_api.utils.models import ClassificationData
from documentai_api.utils.response_codes import ResponseCodes

logger = get_logger(__name__)


@dataclass
class MatchedBlueprintInfo:
    """Timing data calculated during BDA processing completion."""

    name: str
    confidence: float | None


@dataclass
class BdaProcessingResults:
    """Data elements derrived from BDA output."""

    empty_field_list: list[str] = field(default_factory=list)
    field_confidence_map_list: list[dict[str, float]] = field(default_factory=list)
    response_code: str | None = None


def get_bda_processing_results(bda_result_json: dict[str, Any]) -> BdaProcessingResults:
    """Extract field processing results from BDA output."""
    if BdaResponseFields.EXPLAINABILITY_INFO not in bda_result_json:
        return BdaProcessingResults(response_code=ResponseCodes.INTERNAL_PROCESSING_ERROR)

    field_data = extract_field_metadata_from_bda_results(bda_result_json)
    response_code = _determine_response_code(field_data)

    return BdaProcessingResults(
        field_confidence_map_list=field_data.field_confidence_map_list,
        empty_field_list=field_data.empty_fields,
        response_code=response_code,
    )


def _determine_response_code(field_data: BdaFieldProcessingData) -> str:
    """Determine response code based on field results."""
    # add logic here if response code should be derived from field data
    # returning success as default
    return ResponseCodes.SUCCESS


def get_matched_blueprint(bda_result_json: dict[str, Any]) -> MatchedBlueprintInfo:
    """Extract matched blueprint name and confidence from BDA result JSON."""
    matched_blueprint = bda_result_json.get(BdaResponseFields.MATCHED_BLUEPRINT, {})
    matched_blueprint_name = matched_blueprint.get(BdaResponseFields.MATCHED_BLUEPRINT_NAME)
    matched_blueprint_confidence = matched_blueprint.get(
        BdaResponseFields.MATCHED_BLUEPRINT_CONFIDENCE
    )

    return MatchedBlueprintInfo(matched_blueprint_name, matched_blueprint_confidence)


def process_bda_output(
    uploaded_filename: str, bda_output_bucket_name: str, bda_output_object_key: str
) -> dict[str, Any]:
    user_provided_document_category = get_user_provided_document_category(uploaded_filename)

    if not user_provided_document_category:
        msg = "No user specified document type provided. Document not implemented"
        logger.info(msg)

        return classify_as_not_implemented(
            object_key=uploaded_filename,
            data=ClassificationData(additional_info=msg),
        )

    bda_output_s3_uri = extract_bda_output_s3_uri(bda_output_bucket_name, bda_output_object_key)

    if not bda_output_s3_uri:
        raise ValueError("No BDA output S3 URI found")

    bda_result_json = get_bda_result_json(bda_output_s3_uri)
    if not bda_result_json:
        raise ValueError("No BDA result JSON found")

    matched_blueprint = get_matched_blueprint(bda_result_json)

    document_class = bda_result_json.get(BdaResponseFields.DOCUMENT_CLASS, {}).get(
        BdaResponseFields.DOCUMENT_TYPE
    )

    classification_data = ClassificationData(
        bda_output_s3_uri=bda_output_s3_uri,
        matched_document_class=document_class,
        matched_blueprint_name=matched_blueprint.name,
        matched_blueprint_confidence=matched_blueprint.confidence,
    )

    logger.debug(f"Matched blueprint: {matched_blueprint.name}")

    if matched_blueprint.name is None:
        msg = "No matching custom blueprint found. "
        text = get_text_from_standard_blueprint(bda_result_json)

        if text and len([c for c in text if c.isalnum()]) > int(
            ConfigDefaults.BDA_DOCUMENT_DETECTION_MIN_CHAR_LENGTH.value
        ):
            msg += "Document detected, but not implemented."
            logger.info(msg)
            classification_data.additional_info = msg
            return classify_as_no_custom_blueprint_matched(
                object_key=uploaded_filename, data=classification_data
            )
        else:
            msg += "Unable to extract meaningful document content."
            logger.info(msg)
            classification_data.additional_info = msg
            return classify_as_no_document_detected(
                object_key=uploaded_filename, data=classification_data
            )
    else:
        msg = "Custom matching blueprint found, and document type matches. Success."
        logger.info(msg)
        results = get_bda_processing_results(bda_result_json)

        classification_data.field_confidence_scores = results.field_confidence_map_list
        classification_data.field_empty_list = results.empty_field_list
        classification_data.additional_info = msg

        return classify_as_success(
            object_key=uploaded_filename,
            response_code=results.response_code or ResponseCodes.SUCCESS,
            data=classification_data,
        )


__all__ = ["process_bda_output"]
