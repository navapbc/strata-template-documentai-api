from typing import Any

from pydantic import AwareDatetime, HttpUrl

from documentai_api.models.base import CamelCaseResponse


class UploadAsyncResponse(CamelCaseResponse):
    job_id: str
    job_status: str
    message: str


class JobStatusResponse(CamelCaseResponse):
    job_id: str
    job_status: str
    message: str
    created_at: AwareDatetime | None = None
    completed_at: AwareDatetime | None = None
    total_processing_time_seconds: float | None = None
    matched_document_class: str | None = None
    fields: dict[str, Any] | None = None
    error: str | None = None
    additional_info: str | None = None


class HealthResponse(CamelCaseResponse):
    message: str


class ConfigResponse(CamelCaseResponse):
    api_url: HttpUrl
    version: str
    image_tag: str | None
    environment: str
    endpoints: dict[str, str]
    supported_file_types: list[str]


class SchemaListResponse(CamelCaseResponse):
    schemas: list[str]


class SchemaFieldResponse(CamelCaseResponse):
    name: str
    type: str
    description: str


class SchemaDetailResponse(CamelCaseResponse):
    document_type: str
    fields: list[SchemaFieldResponse]
