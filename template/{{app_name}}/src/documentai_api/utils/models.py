"""Data models for document classification and field metrics."""

from dataclasses import dataclass

from documentai_api.config.constants import DocumentCategory


@dataclass
class InternalApiResponse:
    """Shared API response model."""

    validation_passed: bool
    document_category: DocumentCategory
    matched_document_class: str
    response_code: str
    response_message: str


@dataclass
class ClassificationData:
    """Data required for document classification operations."""

    bda_output_s3_uri: str | None = None
    matched_document_class: str | None = None
    matched_blueprint_name: str | None = None
    matched_blueprint_confidence: float | None = None
    field_confidence_scores: list[dict] | None = None
    field_below_threshold_list: list | None = None
    field_empty_list: list | None = None
    additional_info: str | None = None


@dataclass
class FieldMetrics:
    """Field count and confidence metrics for BDA processing."""

    field_count: int
    field_count_not_empty: int
    field_not_empty_avg_confidence: float | None


@dataclass
class ProcessingTimes:
    """Timing data calculated during BDA processing completion."""

    total_processing_time_seconds: float = 0.0
    bda_processing_time_seconds: float = 0.0


@dataclass
class PageMetadata:
    """Metadata for a multipage document page."""

    page_number: int
    s3_key: str
    s3_bucket_name: str
    category: str | None = None
    uploaded_at: str | None = None
