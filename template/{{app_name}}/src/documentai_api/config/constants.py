import json
from enum import Enum
from pathlib import Path


def load_settings():
    config_path = Path(__file__).parent / "constants.json"
    with open(config_path) as f:
        return json.load(f)


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

# ----- generate enums dynamically from settings -----
BdaJobStatus = Enum(
    "BdaJobStatus",
    {key.upper(): value for key, value in SETTINGS["bda_job_statuses"]["all"].items()},
    type=str,
)

BdaResponseFields = Enum(
    "BdaResponseFields",
    {key.upper(): value for key, value in SETTINGS["bda_response_fields"].items()},
    type=str,
)

ConfigDefaults = Enum(
    "ConfigDefaults",
    {key.upper(): value for key, value in SETTINGS["config_defaults"].items()},
    type=str,
)

DocumentCategory = Enum(
    "DocumentCategory",
    {category.upper(): category for category in SETTINGS["document_categories"]},
    type=str,
)

ProcessStatus = Enum(
    "ProcessStatus",
    {key.upper(): value for key, value in SETTINGS["processing_statuses"]["all"].items()},
    type=str,
)
