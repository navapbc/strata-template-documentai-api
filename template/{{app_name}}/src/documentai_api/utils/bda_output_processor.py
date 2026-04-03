from dataclasses import dataclass, field

from documentai_api.config.constants import BdaResponseFields, ConfigDefaults
from documentai_api.services.bda import extract_bda_output_s3_uris, get_bda_result_json
from documentai_api.utils.bda import (
    BdaFieldProcessingData,
    extract_field_metadata_from_bda_results,
    get_text_from_standard_blueprint,
)
from documentai_api.utils.ddb import (
    ClassificationData,
    classify_as_multi_segment,
    classify_as_no_custom_blueprint_matched,
    classify_as_no_document_detected,
    classify_as_not_implemented,
    classify_as_success,
    get_user_provided_document_category,
)
from documentai_api.utils.logger import get_logger
from documentai_api.utils.models import SegmentResult
from documentai_api.utils.response_codes import ResponseCodes

logger = get_logger(__name__)


@dataclass
class MatchedBlueprintInfo:
    """Timing data calculated during BDA processing completion."""

    name: str
    confidence: str


@dataclass
class BdaProcessingResults:
    """Data elements derrived from BDA output."""

    empty_field_list: list = field(default_factory=list)
    field_confidence_map_list: list = field(default_factory=list)
    response_code: str | None = None


def get_bda_processing_results(bda_result_json: dict) -> BdaProcessingResults:
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


def _process_bda_output_segment(
    segment_index: int, uri: str, bda_result_json: dict
) -> SegmentResult:
    """Process a single BDA output segment."""
    matched_blueprint = get_matched_blueprint(bda_result_json)
    document_class = bda_result_json.get(BdaResponseFields.DOCUMENT_CLASS, {}).get(
        BdaResponseFields.DOCUMENT_TYPE
    )

    status = "success"
    additional_info = None
    field_confidence_scores = None
    field_empty_list = None

    if matched_blueprint.name is None:
        text = get_text_from_standard_blueprint(bda_result_json)
        if text and len([c for c in text if c.isalnum()]) > int(
            ConfigDefaults.BDA_DOCUMENT_DETECTION_MIN_CHAR_LENGTH.value
        ):
            status = "no_custom_blueprint_matched"
            additional_info = (
                "No matching custom blueprint found. Document detected, but not implemented."
            )
        else:
            status = "no_document_detected"
            additional_info = (
                "No matching custom blueprint found. Unable to extract meaningful document content."
            )
    else:
        results = get_bda_processing_results(bda_result_json)
        field_confidence_scores = results.field_confidence_map_list
        field_empty_list = results.empty_field_list
        additional_info = "Custom matching blueprint found, and document type matches. Success."

    return SegmentResult(
        segment_index=segment_index,
        bda_output_s3_uri=uri,
        matched_document_class=document_class,
        matched_blueprint_name=matched_blueprint.name,
        matched_blueprint_confidence=matched_blueprint.confidence,
        field_confidence_scores=field_confidence_scores,
        field_empty_list=field_empty_list,
        status=status,
        additional_info=additional_info,
    )


def _determine_response_code(field_data: BdaFieldProcessingData) -> str:
    """Determine response code based on field results."""
    # add logic here if response code should be derived from field data
    # returning success as default
    return ResponseCodes.SUCCESS


def get_matched_blueprint(bda_result_json: dict) -> MatchedBlueprintInfo:
    """Extract matched blueprint name and confidence from BDA result JSON."""
    matched_blueprint = bda_result_json.get(BdaResponseFields.MATCHED_BLUEPRINT, {})
    matched_blueprint_name = matched_blueprint.get(BdaResponseFields.MATCHED_BLUEPRINT_NAME)
    matched_blueprint_confidence = matched_blueprint.get(
        BdaResponseFields.MATCHED_BLUEPRINT_CONFIDENCE
    )

    return MatchedBlueprintInfo(matched_blueprint_name, matched_blueprint_confidence)


def process_bda_output(uploaded_filename, bda_output_bucket_name, bda_output_object_key):
    user_provided_document_category = get_user_provided_document_category(uploaded_filename)

    if not user_provided_document_category:
        msg = "No user specified document type provided. Document not implemented"
        logger.info(msg)
        return classify_as_not_implemented(
            object_key=uploaded_filename,
            data=ClassificationData(additional_info=msg),
        )

    bda_output_s3_uris = extract_bda_output_s3_uris(bda_output_bucket_name, bda_output_object_key)

    segments = []
    for segment_index, uri in enumerate(bda_output_s3_uris):
        bda_result_json = get_bda_result_json(uri)
        segments.append(_process_bda_output_segment(segment_index, uri, bda_result_json))

    if len(segments) == 1:
        segment = segments[0]
        classification_data = ClassificationData(
            bda_output_s3_uri=segment.bda_output_s3_uri,
            matched_document_class=segment.matched_document_class,
            matched_blueprint_name=segment.matched_blueprint_name,
            matched_blueprint_confidence=segment.matched_blueprint_confidence,
            field_confidence_scores=segment.field_confidence_scores,
            field_empty_list=segment.field_empty_list,
            additional_info=segment.additional_info,
        )

        if segment.status == "no_document_detected":
            return classify_as_no_document_detected(
                object_key=uploaded_filename, data=classification_data
            )
        elif segment.status == "no_custom_blueprint_matched":
            return classify_as_no_custom_blueprint_matched(
                object_key=uploaded_filename, data=classification_data
            )
        else:
            return classify_as_success(
                object_key=uploaded_filename,
                response_code=ResponseCodes.SUCCESS,
                data=classification_data,
            )

    # multiple segments
    return classify_as_multi_segment(
        object_key=uploaded_filename,
        segments=segments,
    )


__all__ = ["process_bda_output"]
