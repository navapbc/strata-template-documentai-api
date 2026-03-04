#!/usr/bin/env python3
"""Process BDA output from S3 and extract document data."""

from documentai_api.config.constants import BDA_PROCESSED_FILE_PREFIX, S3Prefix
from documentai_api.utils.bda_output_processor import process_bda_output
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def extract_uploaded_filename(object_key: str) -> str:
    """Extract uploaded filename from BDA output path.

    BDA output: processed/input/w2-xxx.pdf/uuid/0/custom_output/0/result.json
    Extract: w2-xxx.pdf
    """
    filename = (
        object_key.removeprefix(f"{BDA_PROCESSED_FILE_PREFIX}/")
        .removeprefix(f"{S3Prefix.INPUT}/")
        .split("/")[0]
    )

    # map truncated filename back to original
    # TODO: Make truncated filename mapping more robust
    # (handle edge cases like files already containing "_truncated")
    if "_truncated." in filename:
        filename = filename.replace("_truncated.", ".")

    return filename


def main(bucket_name: str, object_key: str) -> dict:
    """Process BDA output file.

    Args:
        bucket_name: S3 bucket containing BDA output
        object_key: S3 object key of BDA output file

    Returns:
        API response data dictionary
    """
    logger.info(f"Processing BDA output: s3://{bucket_name}/{object_key}")

    # only process BDA output job metadata files
    if not object_key.endswith("job_metadata.json"):
        logger.info(f"Skipping non-metadata file: {object_key}")
        return {}

    uploaded_filename = extract_uploaded_filename(object_key)
    logger.info(f"Extracted original filename: {uploaded_filename}")

    result = process_bda_output(uploaded_filename, bucket_name, object_key)
    logger.info(f"Successfully processed BDA output for {uploaded_filename}")

    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        logger.error(
            "Usage: python -m documentai_api.jobs.bda_result_processor.main <bucket_name> <object_key>"
        )
        sys.exit(1)

    bucket_name = sys.argv[1]
    object_key = sys.argv[2]
    main(bucket_name, object_key)
