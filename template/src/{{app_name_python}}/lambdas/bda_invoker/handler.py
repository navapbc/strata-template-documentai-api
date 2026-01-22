import os
from config.constants import ProcessStatus
from schemas.document_metadata import DocumentMetadata
from utils.bda_invoker import invoke_bedrock_data_automation
from utils.ddb import (
    ClassificationData,
    classify_as_failed,
    set_bda_processing_status_started,
)
from utils.env import DDE_INPUT_LOCATION
from utils.error_handling import handle_lambda_errors 


@handle_lambda_errors
def handler(event, context):
    for record in event["Records"]:
        print(f"Processing record: {record['eventName']}")

        # extract key fields from name from ddb stream record
        file_name = record["dynamodb"]["NewImage"]["fileName"]["S"]
        process_status = (
            record["dynamodb"]["NewImage"]
            .get(DocumentMetadata.PROCESS_STATUS, {})
            .get("S")
        )

        if process_status != ProcessStatus.NOT_STARTED.value:
            print(f"Skipping {file_name} - status: {process_status}")
            continue

        bucket_name = os.getenv(DDE_INPUT_LOCATION).replace("s3://", "")

        print(f"Processing new file: {file_name}")

        try:
            invocation_arn = invoke_bedrock_data_automation(bucket_name, file_name)

            set_bda_processing_status_started(
                object_key=file_name,
                bda_invocation_arn=invocation_arn,
            )

            print(f"BDA job started for {file_name}, ARN: {invocation_arn}")

        except Exception as e:
            print(f"BDA invocation failed {file_name}: {e}")
            classify_as_failed(
                object_key=file_name,
                error_message="BDA invocation failed",
                data=ClassificationData(additional_info=str(e)),
            )

            # do not raise an exception here, else the entire batch fails and is retried
            # continue processing subsequent records in the batch
            # note: the batch size is likely to be 1 for DDB streams, but just in case

    return {"statusCode": 200}
