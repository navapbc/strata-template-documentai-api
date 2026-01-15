"""Bedrock Data Automation service methods"""
import json
from config.constants import BdaJobStatus, BDA_JOB_STATUS_RUNNING, BDA_JOB_STATUS_FAILED, BDA_JOB_STATUS_COMPLETED
from utils.aws_client_factory import AWSClientFactory

def invoke_data_automation_async(project_arn: str, input_config: dict, output_config: dict) -> dict:
    """Invoke BDA job asynchronously"""
    bedrock_client = AWSClientFactory.get_bedrock_data_automation_runtime_client()
    
    return bedrock_client.invoke_data_automation_async(
        projectArn=project_arn,
        inputConfiguration=input_config,
        outputConfiguration=output_config
    )

def get_data_automation_job(job_arn: str) -> dict:
    """Get BDA job status"""
    bedrock_client = AWSClientFactory.get_bedrock_data_automation_runtime_client()
    
    return bedrock_client.get_data_automation_job(jobArn=job_arn)


def get_bda_result_json(bda_result_uri: str) -> dict | None:
    """Read and return BDA result JSON from S3"""
    if not bda_result_uri:
        return None

    try:
        s3_parts = bda_result_uri.replace("s3://", "").split("/", 1)
        result_bucket = s3_parts[0]
        result_key = s3_parts[1]

        s3 = AWSClientFactory.get_s3_client()
        bda_result_object = s3.get_object(Bucket=result_bucket, Key=result_key)
        bda_result_json = json.loads(bda_result_object["Body"].read().decode("utf-8"))

        return bda_result_json
    except Exception as e:
        print(f"Failed to read result JSON: {e}")
        return None
    
def get_bda_job_response(bda_invocation_arn: str) -> str | None:
    """Get BDA job status"""
    try:
        bedrock_client = AWSClientFactory.get_bda_runtime_client()
        return bedrock_client.get_data_automation_status(invocationArn=bda_invocation_arn)
    except Exception:
        return None

def extract_bda_output_s3_uri(bda_output_bucket_name: str, bda_output_object_key: str) -> str | None:
    """Read and parse BDA job metadata from S3"""
    s3 = AWSClientFactory.get_s3_client()
    metadata_response = s3.get_object(Bucket=bda_output_bucket_name, Key=bda_output_object_key)
    job_metadata = json.loads(metadata_response["Body"].read().decode("utf-8"))

    # extract bda result uri from job metadata
    try:
        for output_meta in job_metadata.get("output_metadata", []):
            for segment in output_meta.get("segment_metadata", []):
                if "custom_output_path" in segment:
                    return segment["custom_output_path"]

                if "standard_output_path" in segment:
                    return segment["standard_output_path"]
    except (TypeError, AttributeError) as e:
        print(f"Failed to extract BDA result uri: {e}")
        return None
    
