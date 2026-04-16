"""Bedrock Data Automation service methods."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from documentai_api.logging import get_logger
from documentai_api.utils.aws_client_factory import AWSClientFactory

if TYPE_CHECKING:
    from mypy_boto3_bedrock_data_automation.type_defs import (
        GetBlueprintResponseTypeDef,
        GetDataAutomationProjectResponseTypeDef,
    )
    from mypy_boto3_bedrock_data_automation_runtime.type_defs import (
        GetDataAutomationStatusResponseTypeDef,
    )

logger = get_logger(__name__)


def get_data_automation_project(project_arn: str) -> GetDataAutomationProjectResponseTypeDef:
    """Get BDA project details including blueprints."""
    bedrock_client = AWSClientFactory.get_bda_client()
    logger.debug(f"Getting BDA project details for project ARN: {project_arn}")
    return bedrock_client.get_data_automation_project(projectArn=project_arn)


def get_blueprint(blueprint_arn: str) -> GetBlueprintResponseTypeDef:
    """Get blueprint schema details."""
    bedrock_client = AWSClientFactory.get_bda_client()
    return bedrock_client.get_blueprint(blueprintArn=blueprint_arn)


def get_bda_result_json(bda_result_uri: str) -> dict[str, Any] | None:
    """Read and return BDA result JSON from S3."""
    if not bda_result_uri:
        return None

    try:
        s3_parts = bda_result_uri.replace("s3://", "").split("/", 1)
        result_bucket = s3_parts[0]
        result_key = s3_parts[1]

        s3 = AWSClientFactory.get_s3_client()
        bda_result_object = s3.get_object(Bucket=result_bucket, Key=result_key)
        bda_result_json = json.loads(bda_result_object["Body"].read().decode("utf-8"))

        return cast(dict[str, Any], bda_result_json)
    except Exception as e:
        logger.error(f"Failed to read result JSON: {e}")
        return None


def get_bda_job_response(bda_invocation_arn: str) -> GetDataAutomationStatusResponseTypeDef | None:
    """Get BDA job status."""
    try:
        bedrock_client = AWSClientFactory.get_bda_runtime_client()
        return bedrock_client.get_data_automation_status(invocationArn=bda_invocation_arn)
    except Exception:
        return None


def extract_bda_output_s3_uri(
    bda_output_bucket_name: str, bda_output_object_key: str
) -> str | None:
    """Read and parse BDA job metadata from S3."""
    s3 = AWSClientFactory.get_s3_client()
    metadata_response = s3.get_object(Bucket=bda_output_bucket_name, Key=bda_output_object_key)
    job_metadata = json.loads(metadata_response["Body"].read().decode("utf-8"))

    # extract bda result uri from job metadata
    try:
        for output_meta in job_metadata.get("output_metadata", []):
            for segment in output_meta.get("segment_metadata", []):
                if "custom_output_path" in segment:
                    return str(segment["custom_output_path"])

                if "standard_output_path" in segment:
                    return str(segment["standard_output_path"])

        return None
    except (TypeError, AttributeError) as e:
        logger.error(f"Failed to extract BDA result uri: {e}")
        return None
