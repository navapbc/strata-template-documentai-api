#!/usr/bin/env python3
"""Invoke Bedrock Data Automation for document processing."""

import argparse
import json
import os
import sys

from config.constants import ProcessStatus
from schemas.document_metadata import DocumentMetadata
from utils.bda_invoker import invoke_bedrock_data_automation
from utils.ddb import (
    ClassificationData,
    classify_as_failed,
    get_ddb_record,
    set_bda_processing_status_started,
)
from utils.env import DDE_INPUT_LOCATION
from utils.logger import get_logger

logger = get_logger(__name__)

def main(file_name: str, bucket_name: str = None, bypass_ddb_status_check: bool = False) -> dict:
    """Invoke BDA for a file.
    
    Args:
        file_name: Name of file to process
        bucket_name: Optional S3 bucket name (defaults to DDE_INPUT_LOCATION env var)
        bypass_ddb_status_check: Skip checking DDB record status (default: False)
        
    Returns:
        Status dictionary
    """
    if bucket_name is None:
        bucket_name = os.getenv(DDE_INPUT_LOCATION).replace("s3://", "")
    
    logger.info(f"Invoking BDA for file: {file_name} in bucket: {bucket_name}")

    # check ddb record status unless explicitly skipped
    if not bypass_ddb_status_check:
        try:
            record = get_ddb_record(file_name)
            process_status = record.get(DocumentMetadata.PROCESS_STATUS)
            
            if process_status != ProcessStatus.NOT_STARTED.value:
                logger.info(f"Skipping {file_name} - status: {process_status}")
                return {"statusCode": 200, "skipped": True, "reason": f"Status is {process_status}"}
        except ValueError:
            logger.error(f"No DDB record found for {file_name}")
            raise

    try:
        invocation_arn = invoke_bedrock_data_automation(bucket_name, file_name)

        set_bda_processing_status_started(
            object_key=file_name,
            bda_invocation_arn=invocation_arn,
        )

        logger.info(f"BDA job started for {file_name}, ARN: {invocation_arn}")
        return {"statusCode": 200, "invocationArn": invocation_arn}

    except Exception as e:
        logger.error(f"BDA invocation failed for {file_name}: {e}")
        classify_as_failed(
            object_key=file_name,
            error_message="BDA invocation failed",
            data=ClassificationData(additional_info=str(e)),
        )
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invoke BDA for document processing")
    parser.add_argument("--file", required=True, help="File name to process")
    parser.add_argument("--bucket", help="S3 bucket name (defaults to DDE_INPUT_LOCATION)")
    parser.add_argument("--bypass-ddb-status-check", action="store_true",
                    help="Skip checking DDB record status before invoking BDA")

    args = parser.parse_args()
    
    try:
        result = main(args.file, args.bucket, args.bypass_ddb_status_check)
        print(json.dumps(result))
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to invoke BDA: {e}", exc_info=True)
        sys.exit(1)