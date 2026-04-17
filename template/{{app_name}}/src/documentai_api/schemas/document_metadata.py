from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class DocumentMetadata(BaseModel):
    """Pydantic model for DynamoDB document metadata records."""

    model_config = ConfigDict(populate_by_name=True)

    # core fields
    file_name: str = Field(alias="fileName")
    original_file_name: str = Field("", alias="originalFileName")
    user_provided_document_category: str | None = Field(
        "Not specified", alias="userProvidedDocumentCategory"
    )
    process_status: str | None = Field(None, alias="processStatus")
    bda_invocation_arn: str | None = Field(None, alias="bdaInvocationArn")
    bda_output_s3_uri: str | None = Field(None, alias="bdaOutputS3Uri")
    error_message: str | None = Field(None, alias="errorMessage")
    response_json: str | None = Field(None, alias="responseJson")
    response_code: str | None = Field(None, alias="responseCode")
    processed_date: str | None = Field(None, alias="processedDate")
    job_id: str | None = Field(None, alias="jobId")
    trace_id: str | None = Field(None, alias="traceId")
    v1_api_response_json: str | None = Field(None, alias="v1ApiResponseJson")
    created_at: str | None = Field(None, alias="createdAt")
    updated_at: str | None = Field(None, alias="updatedAt")

    # performance tracking
    bda_started_at: str | None = Field(None, alias="bdaStartedAt")
    bda_completed_at: str | None = Field(None, alias="bdaCompletedAt")
    total_processing_time_seconds: Decimal | None = Field(None, alias="totalProcessingTimeSeconds")
    bda_processing_time_seconds: Decimal | None = Field(None, alias="bdaProcessingTimeSeconds")
    bda_wait_time_seconds: Decimal | None = Field(None, alias="bdaWaitTimeSeconds")

    # file metadata
    file_size_bytes: int | None = Field(None, alias="fileSizeBytes")
    content_type: str | None = Field(None, alias="contentType")
    pages_detected: int | None = Field(None, alias="pagesDetected")
    is_document_blurry: bool | None = Field(None, alias="isDocumentBlurry")
    is_password_protected: bool | None = Field(None, alias="isPasswordProtected")
    document_metrics_raw: str | None = Field(None, alias="documentMetricsRaw")
    document_metrics_normalized: str | None = Field(None, alias="documentMetricsNormalized")
    overall_blur_score: Decimal | None = Field(None, alias="overallBlurScore")

    # operational intelligence
    additional_info: str | None = Field(None, alias="additionalInfo")
    retry_count: int | None = Field(None, alias="retryCount")
    field_confidence_scores: str | None = Field(None, alias="fieldConfidenceScores")

    # bda processing info
    bda_region_used: str | None = Field(None, alias="bdaRegionUsed")
    matched_blueprint_name: str | None = Field(None, alias="matchedBlueprintName")
    matched_blueprint_confidence: float | None = Field(None, alias="matchedBlueprintConfidence")
    bda_matched_document_class: str | None = Field(None, alias="bdaMatchedDocumentClass")
    matched_blueprint_field_empty_list: str | None = Field(
        None, alias="matchedBlueprintFieldEmptyList"
    )
    matched_blueprint_field_below_threshold_list: str | None = Field(
        None, alias="matchedBlueprintFieldBelowThresholdList"
    )
    matched_blueprint_field_count: int | None = Field(None, alias="matchedBlueprintFieldCount")
    matched_blueprint_field_count_not_empty: int | None = Field(
        None, alias="matchedBlueprintFieldCountNotEmpty"
    )
    matched_blueprint_field_not_empty_avg_confidence: float | None = Field(
        None, alias="matchedBlueprintFieldNotEmptyAvgConfidence"
    )

    @staticmethod
    def field_alias(field_name: str) -> str:
        """Get the DDB field name (alias) for a Python field name."""
        field = DocumentMetadata.model_fields[field_name]
        alias = field.alias
        if not alias:
            raise ValueError(f"No alias for field: {field_name}")
        return alias
