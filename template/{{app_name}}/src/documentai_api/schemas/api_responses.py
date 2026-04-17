from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CamelCaseResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)


class UploadAsyncResponse(CamelCaseResponse):
    job_id: str = Field(alias="jobId")
    job_status: str = Field(alias="jobStatus")
    message: str


class JobStatusResponse(CamelCaseResponse):
    job_id: str = Field(alias="jobId")
    job_status: str = Field(alias="jobStatus")
    message: str | None = None
    created_at: str | None = Field(None, alias="createdAt")
    completed_at: str | None = Field(None, alias="completedAt")
    total_processing_time_seconds: float | None = Field(None, alias="totalProcessingTimeSeconds")
    matched_document_class: str | None = Field(None, alias="matchedDocumentClass")
    fields: dict[str, Any] | None = None
    error: str | None = None
    additional_info: str | None = Field(None, alias="additionalInfo")


class HealthResponse(CamelCaseResponse):
    message: str


class ConfigResponse(CamelCaseResponse):
    api_url: str = Field(alias="apiUrl")
    version: str
    image_tag: str | None = Field(None, alias="imageTag")
    environment: str
    endpoints: dict[str, str]
    supported_file_types: list[str] = Field(alias="supportedFileTypes")


class SchemaListResponse(CamelCaseResponse):
    schemas: list[str]


class SchemaFieldResponse(CamelCaseResponse):
    name: str
    type: str
    description: str


class SchemaDetailResponse(CamelCaseResponse):
    document_type: str = Field(alias="documentType")
    fields: list[SchemaFieldResponse]
