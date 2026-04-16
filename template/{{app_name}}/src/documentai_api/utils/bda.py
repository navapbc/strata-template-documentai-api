from dataclasses import dataclass
from typing import Any

from documentai_api.config.constants import (
    BDA_JOB_STATUS_COMPLETED,
    BDA_JOB_STATUS_FAILED,
    BDA_JOB_STATUS_RUNNING,
    BdaResponseFields,
)
from documentai_api.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BdaFieldProcessingData:
    confidence_scores: list[float]
    empty_fields: list[str]
    field_confidence_map_list: list[dict[str, float]]


@dataclass
class BdaFieldProcessingResult:
    confidence: float
    is_empty: bool


def is_bda_job_running(status: str) -> bool:
    """Check if BDA job is still running."""
    return status in BDA_JOB_STATUS_RUNNING


def is_bda_job_failed(status: str) -> bool:
    """Check if BDA job has failed."""
    return status in BDA_JOB_STATUS_FAILED


def is_bda_job_completed(status: str) -> bool:
    """Check if BDA job completed successfully."""
    return status in BDA_JOB_STATUS_COMPLETED


def _extract_fields_recursive(
    data: dict[str, Any],
    parent_key: str,
    confidence_scores: list[float],
    empty_fields: list[str],
    field_confidence_map_list: list[dict[str, float]],
    field_values: dict[str, Any] | None = None,
) -> None:
    """Recursively process fields, handling both flat and nested structures."""
    for field_name, field_data in data.items():
        if not isinstance(field_data, dict):
            continue

        full_field_name = f"{parent_key}.{field_name}" if parent_key else field_name

        # check field is an actual extracted value (e.g. confidence score or value) or a nested structure
        if (
            BdaResponseFields.FIELD_CONFIDENCE in field_data
            or BdaResponseFields.FIELD_VALUE in field_data
        ):
            # extracted field - process it
            field_result = _process_single_field(full_field_name, field_data)
            field_confidence_map_list.append({full_field_name: field_result.confidence})

            if field_result.is_empty:
                empty_fields.append(full_field_name)
            else:
                confidence_scores.append(field_result.confidence)

            if field_values is not None:
                field_values[full_field_name] = field_data.get(BdaResponseFields.FIELD_VALUE)
        else:
            # nested structure - recursion required
            _extract_fields_recursive(
                field_data,
                full_field_name,
                confidence_scores,
                empty_fields,
                field_confidence_map_list,
                field_values,
            )


def _process_single_field(field_name: str, field_data: dict[str, Any]) -> BdaFieldProcessingResult:
    """Process a single field and return its results."""
    confidence = field_data.get(BdaResponseFields.FIELD_CONFIDENCE, 0)
    value = field_data.get(BdaResponseFields.FIELD_VALUE, "")
    is_empty = len(str(value)) == 0

    logger.info(f"Extracted field name: {field_name}, confidence: {confidence}")

    return BdaFieldProcessingResult(confidence, is_empty)


def get_text_from_standard_blueprint(bda_result_json: dict[str, Any]) -> str | None:
    """Extract text from BDA standard output for both document and image modalities."""
    if not bda_result_json:
        return None

    semantic_modality = bda_result_json.get("metadata", {}).get("semantic_modality")

    if semantic_modality == "DOCUMENT" and bda_result_json.get("pages"):
        page = bda_result_json["pages"][0]
        text = page.get("representation", {}).get("text", "")
        if text:
            return str(text.strip())

    elif semantic_modality == "IMAGE" and bda_result_json.get("image"):
        image_data = bda_result_json["image"]
        text_words = image_data.get("text_words", [])
        words = [word.get("text", "") for word in text_words if word.get("text")]
        text = " ".join(words)
        if text:
            return str(text.strip())

    return None


def extract_field_values_from_bda_results(
    bda_result_json: dict[str, Any],
) -> tuple[BdaFieldProcessingData, dict[str, Any]]:
    """Extract both metadata and field values from BDA result."""
    if BdaResponseFields.EXPLAINABILITY_INFO not in bda_result_json:
        return (BdaFieldProcessingData([], [], []), {})

    explainability_info = bda_result_json[BdaResponseFields.EXPLAINABILITY_INFO]

    confidence_scores: list[float] = []
    empty_fields: list[str] = []
    field_confidence_map_list: list[dict[str, float]] = []
    field_values: dict[str, Any] = {}

    for item in explainability_info:
        if isinstance(item, dict):
            _extract_fields_recursive(
                item, "", confidence_scores, empty_fields, field_confidence_map_list, field_values
            )

    metadata = BdaFieldProcessingData(
        confidence_scores=confidence_scores,
        empty_fields=empty_fields,
        field_confidence_map_list=field_confidence_map_list,
    )

    return (metadata, field_values)


def extract_field_metadata_from_bda_results(
    bda_result_json: dict[str, Any],
) -> BdaFieldProcessingData:
    """Extract only metadata (confidence, empty fields) from BDA result."""
    metadata, _ = extract_field_values_from_bda_results(bda_result_json)
    return metadata
