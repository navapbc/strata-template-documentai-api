import json
import logging
import os
from dataclasses import dataclass, field

from config.constants import BdaResponseFields, ConfigDefaults, DocumentCategory
from utils.response_codes import ResponseCodes
from utils.ddb import (
    ClassificationData,
    classify_as_no_custom_blueprint_matched,
    classify_as_no_document_detected,
    classify_as_not_implemented,
    classify_as_success,
    get_user_provided_document_category,
)


from services.bda import extract_bda_output_s3_uri, get_bda_result_json

logger = logging.getLogger(__name__)


@dataclass
class MatchedBlueprintInfo:
    """Timing data calculated during BDA processing completion"""

    name: str
    confidence: str


@dataclass
class BdaProcessingResults:
    """Data elements derrived from BDA output"""

    empty_field_list: list = field(default_factory=list)
    field_confidence_score_list: list = field(default_factory=list)
    response_code: str | None = None

@dataclass
class BdaFieldProcessingData:
    confidence_scores: list
    empty_fields: list
    field_confidence_score_list: list
    
@dataclass
class BdaFieldProcessingResult:
    confidence: float
    is_empty: bool


def get_text_from_standard_blueprint(bda_result_json):
    """Extract text from BDA standard output for both document and image modalities"""
    if not bda_result_json:
        return None
    
    semantic_modality = bda_result_json.get("metadata", {}).get("semantic_modality")
    
    if semantic_modality == "DOCUMENT" and bda_result_json.get("pages"):
        page = bda_result_json["pages"][0]
        text = page.get("representation", {}).get("text", "")
        if text:
            return text.strip()
    
    elif semantic_modality == "IMAGE" and bda_result_json.get("image"):
        image_data = bda_result_json["image"]
        text_words = image_data.get("text_words", [])
        words = [word.get("text", "") for word in text_words if word.get("text")]
        text = " ".join(words)
        if text:
            return text.strip()
    
    return None


def get_bda_processing_results(bda_result_json: dict) -> BdaProcessingResults:
    """Extract field processing results from BDA output."""
    if BdaResponseFields.EXPLAINABILITY_INFO not in bda_result_json:
        return BdaProcessingResults(response_code=ResponseCodes.INTERNAL_PROCESSING_ERROR)
    
    field_data = _extract_field_data(bda_result_json)
    response_code = _determine_response_code(field_data)
    
    return BdaProcessingResults(
        field_confidence_score_list=field_data.field_confidence_score_list,
        empty_field_list=field_data.empty_fields,
        response_code=response_code
    )

def _extract_field_data(bda_result_json: dict) -> BdaFieldProcessingData:
    """Extract and categorize field data from BDA result."""
    explainability_info = bda_result_json[BdaResponseFields.EXPLAINABILITY_INFO]
    
    confidence_scores = []
    empty_fields = []
    field_confidence_score_list = []
    
    for item in explainability_info:
        if isinstance(item, dict):
            for field_name, field_data in item.items():
                if isinstance(field_data, dict):
                    field_result = _process_single_field(field_name, field_data)
                    field_confidence_score_list.append({field_name: field_result.confidence})

                    if field_result.is_empty:
                        empty_fields.append(field_name)
                    else:
                        confidence_scores.append(field_result.confidence)
    
    return BdaFieldProcessingData(
        confidence_scores=confidence_scores,
        empty_fields=empty_fields,
        field_confidence_score_list=field_confidence_score_list
    )


def _process_single_field(field_name: str, field_data: dict) -> BdaFieldProcessingResult:
    """Process a single field and return its results."""
    confidence = field_data.get(BdaResponseFields.FIELD_CONFIDENCE, 0)
    value = field_data.get(BdaResponseFields.FIELD_VALUE, "")
    is_empty = len(str(value)) == 0
    
    msg = f"Extracted field name: {field_name}, confidence: {confidence}"
    print(msg)
    logger.info(msg)
    
    return BdaFieldProcessingResult(confidence, is_empty)

def _determine_response_code(field_data: BdaFieldProcessingData) -> str:
    """Determine response code based on field results."""
    # add logic here if response code should be derived from field data
    # returning success as default
    return ResponseCodes.SUCCESS


def get_matched_blueprint(bda_result_json: dict) -> MatchedBlueprintInfo:
    """Extract matched blueprint name and confidence from BDA result JSON"""

    matched_blueprint = bda_result_json.get(BdaResponseFields.MATCHED_BLUEPRINT, {})
    matched_blueprint_name = matched_blueprint.get(BdaResponseFields.MATCHED_BLUEPRINT_NAME)
    matched_blueprint_confidence = matched_blueprint.get(
        BdaResponseFields.MATCHED_BLUEPRINT_CONFIDENCE
    )

    return MatchedBlueprintInfo(matched_blueprint_name, matched_blueprint_confidence)


def get_api_response_data(uploaded_filename, bda_output_bucket_name, bda_output_object_key):
    user_provided_document_category = get_user_provided_document_category(uploaded_filename)

    if not user_provided_document_category:
        msg = "No user specified document type provided. Document not implemented"
        print(msg)
        logger.info(msg)

        return classify_as_not_implemented(
            object_key=uploaded_filename,
            data=ClassificationData(additional_info=msg),
        )

    bda_output_s3_uri = extract_bda_output_s3_uri(bda_output_bucket_name, bda_output_object_key)
    bda_result_json = get_bda_result_json(bda_output_s3_uri)
    matched_blueprint = get_matched_blueprint(bda_result_json)

    document_class = bda_result_json.get(BdaResponseFields.DOCUMENT_CLASS, {}).get(
        BdaResponseFields.DOCUMENT_TYPE
    )

    classification_data = ClassificationData(
        bda_output_s3_uri=bda_output_s3_uri,
        matched_blueprint_name=matched_blueprint.name,
        matched_blueprint_confidence=matched_blueprint.confidence,
        document_type=document_class,
    )

    print(f"Matched blueprint: {matched_blueprint.name}")

    if matched_blueprint.name is None:
        msg = "No matching custom blueprint found. "
        text = get_text_from_standard_blueprint(bda_result_json)

        if (
            text
            and len([c for c in text if c.isalnum()]) > int(ConfigDefaults.BDA_DOCUMENT_DETECTION_MIN_CHAR_LENGTH.value)
        ):
            msg += "Document detected, but not implemented."
            print(msg)
            logger.info(msg)
            classification_data.additional_info = msg
            return classify_as_no_custom_blueprint_matched(
                object_key=uploaded_filename, data=classification_data
            )
        else:
            msg += "Unable to extract meaningful document content."
            print(msg)
            logger.info(msg)
            classification_data.additional_info = msg
            return classify_as_no_document_detected(
                object_key=uploaded_filename, data=classification_data
            )
    else:
        msg = "Custom matching blueprint found, and document type matches. Success."
        print(msg)
        logger.info(msg)
        idp_info = get_bda_processing_results(bda_result_json)

        classification_data.field_confidence_scores = idp_info.field_confidence_score_list
        classification_data.field_empty_list = idp_info.empty_field_list
        classification_data.additional_info = msg

        return classify_as_success(
            object_key=uploaded_filename,
            response_code=idp_info.response_code,
            data=classification_data,
        )


__all__ = ["get_api_response_data"]
