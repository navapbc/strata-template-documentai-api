import os

from documentai_api.logging import get_logger
from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.env import (
    BDA_PROFILE_ARN,
    BDA_PROJECT_ARN,
    DOCUMENTAI_OUTPUT_LOCATION,
    get_required_env,
)

logger = get_logger(__name__)


def invoke_bedrock_data_automation(source_bucket_name: str, source_object_name: str) -> str:
    """Invoke BDA and return job ARN."""
    bda_project_arn = get_required_env(BDA_PROJECT_ARN)
    bda_profile_arn = get_required_env(BDA_PROFILE_ARN)
    documentai_output_location = get_required_env(DOCUMENTAI_OUTPUT_LOCATION).replace("s3://", "")

    logger.info(f"documentai_output_location after processing: {documentai_output_location}")
    logger.info(f"BDA_PROJECT_ARN: {bda_project_arn}")
    logger.info(f"BDA_PROFILE_ARN: {bda_profile_arn}")

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

        # TODO: refactor to call services/bda.py instead of calling runtime client directly
        response = bedrock.invoke_data_automation_async(
            dataAutomationProfileArn=bda_profile_arn,
            dataAutomationConfiguration={"dataAutomationProjectArn": bda_project_arn},
            inputConfiguration={"s3Uri": f"s3://{source_bucket_name}/{source_object_name}"},
            outputConfiguration={
                "s3Uri": f"s3://{documentai_output_location}/{source_object_name}"
            },
        )
        logger.info(f"BDA response: {response}")
        return str(response.get("invocationArn"))
    except Exception as e:
        logger.error(f"BDA API call failed: {e}")
        raise


__all__ = ["invoke_bedrock_data_automation"]
