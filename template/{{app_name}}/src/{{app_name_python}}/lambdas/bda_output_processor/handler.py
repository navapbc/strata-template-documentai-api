from config.constants import BDA_PROCESSED_FILE_PREFIX
from utils.bda_output_processor import get_api_response_data
from utils.error_handling import handle_lambda_errors
from utils.s3 import extract_s3_info_from_event, validate_s3_event


def extract_uploaded_filename(object_key):
    """Extract uploaded filename from BDA output path."""
    path_parts = object_key.split("/")
    if len(path_parts) >= 2 and path_parts[0] == BDA_PROCESSED_FILE_PREFIX:
        filename = path_parts[1]

        # map truncated filename back to original
        # TODO: Make truncated filename mapping more robust
        # (handle edge cases like files already containing "_truncated")
        if "_truncated." in filename:
            filename = filename.replace("_truncated.", ".")

        return filename

    else:
        raise ValueError(f"Invalid BDA output path: {object_key}")


# error handling managed by @handle_lambda_errors decorator
# event validation managed by @validate_s3_event decorator
@handle_lambda_errors
@validate_s3_event
def handler(event, context):
    bda_output_object_key, bda_output_bucket_name = extract_s3_info_from_event(event)
    uploaded_filename = extract_uploaded_filename(bda_output_object_key)

    return get_api_response_data(uploaded_filename, bda_output_bucket_name, bda_output_object_key)
