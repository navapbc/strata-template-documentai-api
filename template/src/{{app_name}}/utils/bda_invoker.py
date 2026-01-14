import os
import sys

from utils.aws_client_factory import AWSClientFactory

from utils.env import (
    DDE_OUTPUT_LOCATION,
    DDE_PROFILE_ARN,
    DDE_PROJECT_ARN,
)

def invoke_bedrock_data_automation(source_bucket_name, source_object_name):
    """Invoke BDA and return job ARN"""
    dde_project_arn = os.getenv(DDE_PROJECT_ARN)
    dde_profile_arn = os.getenv(DDE_PROFILE_ARN)
    dde_output_location = os.getenv(DDE_OUTPUT_LOCATION).replace("s3://", "")

    print(f"dde_output_location after processing: {dde_output_location}")
    print(f"DDE_PROJECT_ARN: {dde_project_arn}")
    print(f"DDE_PROFILE_ARN: {dde_profile_arn}")

    try:
        bedrock = AWSClientFactory.get_bda_runtime_client()
    except Exception as e:
        print(f"Failed to create bedrock client: {e}")
        raise

    try:
        from utils.document_detector import DocumentDetector, MULTIPAGE_DETECTION_MAX_PAGES  # noqa: E402
        from services import s3 as s3_service

        file_bytes = s3_service.get_file_bytes(source_bucket_name, source_object_name)
        document_detector = DocumentDetector()
        page_count = document_detector.get_page_count(file_bytes)

        if page_count and page_count > MULTIPAGE_DETECTION_MAX_PAGES:
            print(f"{source_object_name} has {page_count} pages, truncating to {MULTIPAGE_DETECTION_MAX_PAGES}")
            
            truncated_bytes = document_detector.truncate_to_pages(file_bytes, max_pages=MULTIPAGE_DETECTION_MAX_PAGES)

            # create new truncated file name
            base_name, extension = os.path.splitext(source_object_name)
            extension = extension or ""  # handle None/empty extension
            source_object_name = f"{base_name}_truncated{extension}"

            # upload truncated version to S3
            s3_service.put_object(
                bucket=source_bucket_name,
                key=source_object_name,
                body=truncated_bytes
            )

        print(f"BDA API call parameters:")
        print(f"  dataAutomationProfileArn: {dde_profile_arn}")
        print(f"  dataAutomationProjectArn: {dde_project_arn}")
        print(f"  inputConfiguration: s3://{source_bucket_name}/{source_object_name}")
        print(f"  outputConfiguration: s3://{dde_output_location}/processed/{source_object_name}")
        response = bedrock.invoke_data_automation_async(
            dataAutomationProfileArn=dde_profile_arn,
            dataAutomationConfiguration={"dataAutomationProjectArn": dde_project_arn},
            inputConfiguration={"s3Uri": f"s3://{source_bucket_name}/{source_object_name}"},
            outputConfiguration={
                "s3Uri": f"s3://{dde_output_location}/processed/{source_object_name}"
            },
        )
        print(f"BDA response: {response}")
        return response.get("invocationArn")
    except Exception as e:
        print(f"BDA API call failed: {e}")
        raise


__all__ = ["invoke_bedrock_data_automation"]
