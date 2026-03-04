import os

from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.env import (
    DOCUMENTAI_OUTPUT_LOCATION,
    DOCUMENTAI_PROFILE_ARN,
    DOCUMENTAI_PROJECT_ARN,
)
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def invoke_bedrock_data_automation(source_bucket_name, source_object_name):
    """Invoke BDA and return job ARN."""
    documentai_project_arn = os.getenv(DOCUMENTAI_PROJECT_ARN)
    documentai_profile_arn = os.getenv(DOCUMENTAI_PROFILE_ARN)
    documentai_output_location = os.getenv(DOCUMENTAI_OUTPUT_LOCATION).replace("s3://", "")

    logger.info(f"documentai_output_location after processing: {documentai_output_location}")
    logger.info(f"DOCUMENTAI_PROJECT_ARN: {documentai_project_arn}")
    logger.info(f"DOCUMENTAI_PROFILE_ARN: {documentai_profile_arn}")

    try:
        bedrock = AWSClientFactory.get_bda_runtime_client()
    except Exception as e:
        logger.error(f"Failed to create bedrock client: {e}")
        raise

    try:
        from documentai_api.services import s3 as s3_service
        from documentai_api.utils.document_detector import (
            MULTIPAGE_DETECTION_MAX_PAGES,
            DocumentDetector,
        )

        file_bytes = s3_service.get_file_bytes(source_bucket_name, source_object_name)
        document_detector = DocumentDetector()
        page_count = document_detector.get_page_count(file_bytes)

        if page_count and page_count > MULTIPAGE_DETECTION_MAX_PAGES:
            logger.info(
                f"{source_object_name} has {page_count} pages, truncating to {MULTIPAGE_DETECTION_MAX_PAGES}"
            )

            truncated_bytes = document_detector.truncate_to_pages(
                file_bytes, max_pages=MULTIPAGE_DETECTION_MAX_PAGES
            )

            # create new truncated file name
            base_name, extension = os.path.splitext(source_object_name)
            extension = extension or ""  # handle None/empty extension
            source_object_name = f"{base_name}_truncated{extension}"

            # upload truncated version to S3
            s3_service.put_object(
                bucket=source_bucket_name, key=source_object_name, body=truncated_bytes
            )

        response = bedrock.invoke_data_automation_async(
            dataAutomationProfileArn=documentai_profile_arn,
            dataAutomationConfiguration={"dataAutomationProjectArn": documentai_project_arn},
            inputConfiguration={"s3Uri": f"s3://{source_bucket_name}/{source_object_name}"},
            outputConfiguration={
                "s3Uri": f"s3://{documentai_output_location}/processed/{source_object_name}"
            },
        )
        logger.info(f"BDA response: {response}")
        return response.get("invocationArn")
    except Exception as e:
        logger.error(f"BDA API call failed: {e}")
        raise


__all__ = ["invoke_bedrock_data_automation"]
