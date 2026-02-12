"""Lambda handler for invoking BDA on DDB stream events."""

import os

from documentai_api.config.constants import ProcessStatus
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.tasks.bda_invoker.main import main as invoke_bda_main
from documentai_api.utils.env import DDE_INPUT_LOCATION
from documentai_api.utils.error_handling import handle_lambda_errors
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


@handle_lambda_errors
def handler(event, _context):
    """Lambda handler for DDB stream events to invoke BDA."""
    bucket_name = os.getenv(DDE_INPUT_LOCATION).replace("s3://", "")

    for record in event["Records"]:
        logger.info(f"Processing record: {record['eventName']}")

        # extract file name and status from ddb stream record
        file_name = record["dynamodb"]["NewImage"]["fileName"]["S"]
        process_status = (
            record["dynamodb"]["NewImage"].get(DocumentMetadata.PROCESS_STATUS, {}).get("S")
        )

        if process_status != ProcessStatus.NOT_STARTED.value:
            logger.info(f"Skipping {file_name} - status: {process_status}")
            continue

        try:
            # call the CLI script's main function
            # bypassing ddb check as this is invoked via ddb streams
            result = invoke_bda_main(file_name, bucket_name, bypass_ddb_status_check=True)
            logger.info(f"Successfully invoked BDA for {file_name}: {result}")
        except Exception as e:
            logger.error(f"Failed to invoke BDA for {file_name}: {e}")
            # do not raise exception - continue processing other records in batch

    return {"statusCode": 200}
