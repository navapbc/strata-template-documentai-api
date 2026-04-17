#!/usr/bin/env python3
"""Process BDA output from S3 and extract document data."""

import json
from typing import Any

import typer

from documentai_api.logging import get_logger
from documentai_api.utils import env
from documentai_api.utils.bda_output_processor import process_bda_output
from documentai_api.utils.env import get_required_env
from documentai_api.utils.s3 import get_s3_prefix_from_location

logger = get_logger(__name__)
app = typer.Typer()


def extract_uploaded_filename(object_key: str) -> str:
    """Extract uploaded filename from BDA output path.

    BDA output: processed/input/w2-xxx.pdf/uuid/0/custom_output/0/result.json
    Extract: w2-xxx.pdf
    """
    output_prefix = get_s3_prefix_from_location(get_required_env(env.DOCUMENTAI_OUTPUT_LOCATION))
    input_prefix = get_s3_prefix_from_location(get_required_env(env.DOCUMENTAI_INPUT_LOCATION))

    # remove prefixes: processed/input/filename.pdf/... -> filename.pdf
    filename = object_key

    if output_prefix:
        filename = filename.removeprefix(f"{output_prefix}/")

    if input_prefix:
        filename = filename.removeprefix(f"{input_prefix}/")

    # get first path component (the filename)
    filename = filename.split("/")[0]

    # map truncated filename back to original
    # TODO: Make truncated filename mapping more robust
    # (handle edge cases like files already containing "_truncated")
    if "_truncated." in filename:
        filename = filename.replace("_truncated.", ".")

    return filename


def main(bucket_name: str, object_key: str) -> dict[str, Any]:
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


@app.command()
def cli(
    bucket_name: str = typer.Argument(..., help="S3 bucket containing BDA output"),
    object_key: str = typer.Argument(..., help="S3 object key of BDA output file"),
) -> None:
    """Process BDA output file."""
    try:
        result = main(bucket_name, object_key)
        if result:
            typer.echo(json.dumps(result))
    except Exception:
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
