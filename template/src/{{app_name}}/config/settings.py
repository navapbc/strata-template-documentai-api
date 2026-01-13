import json
from enum import Enum
from pathlib import Path


def load_settings():
    config_path = Path(__file__).parent / "settings.json"
    with open(config_path) as f:
        return json.load(f)


SETTINGS = load_settings()
API_VERSION = SETTINGS["api"]["version"]
DEFAULT_TIMEOUT = SETTINGS["api"]["default_timeout"]
SUPPORTED_CONTENT_TYPES = SETTINGS["file_validation"]["supported_content_types"]
DOCUMENT_CATEGORIES = SETTINGS["document_categories"]
UPLOAD_METADATA_KEYS = SETTINGS["upload_metadata_keys"]

# individual processing statuses
PROCESSING_STATUS_SUCCESS = SETTINGS["processing_statuses"]["all"]["success"]
PROCESSING_STATUS_NOT_STARTED = SETTINGS["processing_statuses"]["all"]["not_started"]

# grouped processing statuses
PROCESSING_STATUS_COMPLETED = SETTINGS["processing_statuses"]["completed"]
PROCESSING_STATUS_NOT_SUPPORTED = SETTINGS["processing_statuses"]["not_supported"]
PROCESSING_STATUS_PENDING_EXTRACTION = SETTINGS["processing_statuses"]["pending_extraction"]

# ----- generate enums dynamically from settings -----
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