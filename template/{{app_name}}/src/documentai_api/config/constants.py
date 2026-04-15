from enum import StrEnum


class APIConfig:
    VERSION = "v1"
    TITLE = "Document AI API"
    DESCRIPTION = "API for document processing"
    AUTH_KEY_HEADER_NAME = "API-Key"
    DEFAULT_TIMEOUT = 30


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


class Cache:
    KEY_BLUEPRINT_SCHEMAS = "blueprint_schemas"
    TTL_BLUEPRINT_SCHEMAS_MINUTES = 60


class ConfigDefaults:
    FIELD_CONFIDENCE_THRESHOLD = 0.7
    POLL_INTERVAL_SECONDS = 5
    MAX_WAIT_SECONDS = 120
    ALB_TIMEOUT_BUFFER_SECONDS = 15
    USER_DOCUMENT_TYPE_NOT_PROVIDED = "Not specified"
    BDA_REGION_NOT_AVAILABLE = "N/A"
    LOG_RETENTION_DAYS = 30
    BDA_DOCUMENT_DETECTION_MIN_CHAR_LENGTH = 50
    BLURRY_DOCUMENT_THRESHOLD = 25
    BDA_MAX_IMAGE_SIZE_BYTES = 5_242_880
    BDA_MAX_DOCUMENT_FILE_SIZE_BYTES = 524_288_000
    DDB_EMIT_CUSTOM_CLOUDWATCH_METRICS = False
    EMPTY_FIELD_PERCENTAGE_THRESHOLD = 50


class DocumentCategory(StrEnum):
    INCOME = "income"
    EXPENSES = "expenses"
    LEGAL_DOCUMENTS = "legal_documents"
    EMPLOYMENT_TRAINING = "employment_training"


class FileValidation:
    SUPPORTED_CONTENT_TYPES = (
        "application/pdf",
        "image/jpeg",
        "image/png",
        "image/tiff",
    )

    @staticmethod
    def is_supported(content_type: str) -> bool:
        return content_type in FileValidation.SUPPORTED_CONTENT_TYPES


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

    def is_completed(self) -> bool:
        return self in [
            self.SUCCESS,
            self.FAILED,
            self.NO_DOCUMENT_DETECTED,
            self.NO_CUSTOM_BLUEPRINT_MATCHED,
        ]

    def is_not_supported(self) -> bool:
        return self in [
            self.MULTIPAGE,
            self.PASSWORD_PROTECTED,
        ]

    def is_pending_extraction(self) -> bool:
        return self in [self.PENDING_GRAYSCALE_CONVERSION, self.NOT_STARTED]

    def is_successful(self) -> bool:
        return self in [
            self.SUCCESS,
            self.NO_CUSTOM_BLUEPRINT_MATCHED,
            self.NOT_SAMPLED,
            self.NOT_IMPLEMENTED,
        ]


class S3MetadataKeys:
    # S3 metadata keys (for reading from S3 objects)
    USER_PROVIDED_DOCUMENT_CATEGORY = "user-provided-document-category"
    JOB_ID = "job-id"
    TRACE_ID = "trace-id"
    ORIGINAL_FILE_NAME = "original-file-name"
