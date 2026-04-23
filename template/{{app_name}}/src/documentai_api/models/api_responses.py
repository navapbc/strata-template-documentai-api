from typing import Any

from pydantic import AwareDatetime, HttpUrl

from documentai_api.models.base import BaseApiResponse


class UploadAsyncResponse(BaseApiResponse):
    job_id: str
    job_status: str
    message: str


class JobStatusResponse(BaseApiResponse):
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


class HealthResponse(BaseApiResponse):
    message: str


class ConfigResponse(BaseApiResponse):
    api_url: HttpUrl
    version: str
    image_tag: str | None
    environment: str
    endpoints: dict[str, str]
    supported_file_types: list[str]


class SchemaListResponse(BaseApiResponse):
    schemas: list[str]


class SchemaFieldResponse(BaseApiResponse):
    name: str
    type: str
    description: str


class SchemaDetailResponse(BaseApiResponse):
    document_type: str
    fields: list[SchemaFieldResponse]
