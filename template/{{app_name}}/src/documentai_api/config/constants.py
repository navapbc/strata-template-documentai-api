import json
from enum import StrEnum
from pathlib import Path
from typing import Any, cast


def load_settings() -> dict[str, Any]:
    config_path = Path(__file__).parent / "constants.json"
    with open(config_path) as f:
        return cast(dict[str, Any], json.load(f))


SETTINGS = load_settings()
API_VERSION = SETTINGS["api"]["version"]
API_TITLE = SETTINGS["api"]["title"]
API_DESCRIPTION = SETTINGS["api"]["description"]
API_AUTH_KEY_HEADER_NAME = SETTINGS["api"]["auth"]["key"]["header_name"]
DEFAULT_TIMEOUT = SETTINGS["api"]["default_timeout"]
SUPPORTED_CONTENT_TYPES = SETTINGS["file_validation"]["supported_content_types"]
DOCUMENT_CATEGORIES = SETTINGS["document_categories"]
UPLOAD_METADATA_KEYS = SETTINGS["upload_metadata_keys"]

# S3 metadata keys (for reading from S3 objects)
S3_METADATA_KEY_USER_PROVIDED_DOCUMENT_CATEGORY = UPLOAD_METADATA_KEYS[
    "user_provided_document_category"
]
S3_METADATA_KEY_JOB_ID = UPLOAD_METADATA_KEYS["job_id"]
S3_METADATA_KEY_TRACE_ID = UPLOAD_METADATA_KEYS["trace_id"]
S3_METADATA_KEY_ORIGINAL_FILE_NAME = UPLOAD_METADATA_KEYS["original_file_name"]

# grouped processing statuses
PROCESSING_STATUSES_SUCCESSFUL = SETTINGS["processing_statuses"]["successful"]
PROCESSING_STATUS_COMPLETED = SETTINGS["processing_statuses"]["completed"]
PROCESSING_STATUS_NOT_SUPPORTED = SETTINGS["processing_statuses"]["not_supported"]
PROCESSING_STATUS_PENDING_EXTRACTION = SETTINGS["processing_statuses"]["pending_extraction"]

# grouped BDA job statuses
BDA_JOB_STATUS_RUNNING = SETTINGS["bda_job_statuses"]["running"]
BDA_JOB_STATUS_FAILED = SETTINGS["bda_job_statuses"]["failed"]
BDA_JOB_STATUS_COMPLETED = SETTINGS["bda_job_statuses"]["completed"]


# cache
CACHE_KEY_BLUEPRINT_SCHEMAS = SETTINGS["cache"]["blueprint_schemas"]["key"]
CACHE_BLUEPRINT_SCHEMAS_TTL_MINUTES = SETTINGS["cache"]["blueprint_schemas"]["ttl_minutes"]


class BdaJobStatus(StrEnum):
    CREATED = "Created"
    IN_PROGRESS = "InProgress"
    SUCCESS = "Success"
    SERVICE_ERROR = "ServiceError"
    CLIENT_ERROR = "ClientError"


class BdaResponseFields:
    EXPLAINABILITY_INFO = "explainability_info"
    FIELD_CONFIDENCE = "confidence"
    FIELD_VALUE = "value"
    MATCHED_BLUEPRINT = "matched_blueprint"
    MATCHED_BLUEPRINT_NAME = "name"
    MATCHED_BLUEPRINT_CONFIDENCE = "confidence"
    DOCUMENT_CLASS = "document_class"
    DOCUMENT_TYPE = "type"


class ConfigDefaults(StrEnum):
    FIELD_CONFIDENCE_THRESHOLD = "0.7"
    POLL_INTERVAL_SECONDS = "5"
    MAX_WAIT_SECONDS = "120"
    ALB_TIMEOUT_BUFFER_SECONDS = "15"
    USER_DOCUMENT_TYPE_NOT_PROVIDED = "Not specified"
    BDA_REGION_NOT_AVAILABLE = "N/A"
    LOG_RETENTION_DAYS = "30"
    BDA_DOCUMENT_DETECTION_MIN_CHAR_LENGTH = "50"
    BLURRY_DOCUMENT_THRESHOLD = "25"
    BDA_MAX_IMAGE_SIZE_BYTES = "5242880"
    BDA_MAX_DOCUMENT_FILE_SIZE_BYTES = "524288000"
    DDB_EMIT_CUSTOM_CLOUDWATCH_METRICS = "false"
    EMPTY_FIELD_PERCENTAGE_THRESHOLD = "50"


class DocumentCategory(StrEnum):
    INCOME = "income"
    EXPENSES = "expenses"
    LEGAL_DOCUMENTS = "legal_documents"
    EMPLOYMENT_TRAINING = "employment_training"


class ProcessStatus(StrEnum):
    BLURRY_DOCUMENT_DETECTED = "blurry_document_detected"
    FAILED = "failed"
    MULTIPAGE = "multipage"
    NO_CUSTOM_BLUEPRINT_MATCHED = "no_custom_blueprint_matched"
    NO_DOCUMENT_DETECTED = "no_document_detected"
    NOT_IMPLEMENTED = "not_implemented"
    NOT_STARTED = "not_started"
    NOT_SAMPLED = "not_sampled"
    PASSWORD_PROTECTED = "password_protected"
    PENDING_GRAYSCALE_CONVERSION = "pending_grayscale_conversion"
    STARTED = "started"
    SUCCESS = "success"
