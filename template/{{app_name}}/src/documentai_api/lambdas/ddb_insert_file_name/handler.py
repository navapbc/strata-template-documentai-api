"""Lambda handler for processing file uploads from S3 events."""

from documentai_api.config.constants import UPLOAD_METADATA_KEYS
from documentai_api.scripts.ddb_insert_file_name import main as process_upload_main
from documentai_api.utils.error_handling import handle_lambda_errors
from documentai_api.utils.logger import get_logger
from documentai_api.utils.s3 import extract_s3_info_from_event, validate_s3_event

logger = get_logger(__name__)


@handle_lambda_errors
@validate_s3_event
def handler(event, _context):
    """Lambda handler for S3 upload events to insert DDB records."""
    upload_object_key, upload_bucket_name, metadata = extract_s3_info_from_event(
        event, include_metadata=True
    )

    user_provided_document_category = metadata.get(
        UPLOAD_METADATA_KEYS["user_provided_document_category"]
    )
    job_id = metadata.get(UPLOAD_METADATA_KEYS["job_id"])
    trace_id = metadata.get(UPLOAD_METADATA_KEYS["trace_id"])

    logger.info(f"Processing upload: s3://{upload_bucket_name}/{upload_object_key}")

    result = process_upload_main(
        upload_bucket_name,
        upload_object_key,
        user_provided_document_category,
        job_id,
        trace_id,
    )
    logger.info(f"Successfully processed upload: {result}")

    return result
