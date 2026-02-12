#!/usr/bin/env python3
"""Process BDA output from S3 and extract document data."""

from documentai_api.config.constants import BDA_PROCESSED_FILE_PREFIX
from documentai_api.utils.bda_output_processor import get_api_response_data
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def extract_uploaded_filename(object_key: str) -> str:
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


def main(bucket_name: str, object_key: str) -> dict:
    """Process BDA output file.

    Args:
        bucket_name: S3 bucket containing BDA output
        object_key: S3 object key of BDA output file

    Returns:
        API response data dictionary
    """
    logger.info(f"Processing BDA output: s3://{bucket_name}/{object_key}")

    uploaded_filename = extract_uploaded_filename(object_key)
    logger.info(f"Extracted original filename: {uploaded_filename}")

    result = get_api_response_data(uploaded_filename, bucket_name, object_key)
    logger.info(f"Successfully processed BDA output for {uploaded_filename}")

    return result
