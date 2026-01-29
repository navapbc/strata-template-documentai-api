"""Lambda handler for processing BDA output from S3 events."""

from scripts.bda_output_processor import main as process_bda_output_main
from utils.error_handling import handle_lambda_errors
from utils.logger import get_logger
from utils.s3 import extract_s3_info_from_event, validate_s3_event

logger = get_logger(__name__)


@handle_lambda_errors
@validate_s3_event
def handler(event, _context):
    """Lambda handler for S3 events to process BDA output."""
    bda_output_object_key, bda_output_bucket_name = extract_s3_info_from_event(event)

    logger.info(f"Processing BDA output: s3://{bda_output_bucket_name}/{bda_output_object_key}")

    result = process_bda_output_main(bda_output_bucket_name, bda_output_object_key)
    logger.info(f"Successfully processed BDA output: {result}")

    return result
