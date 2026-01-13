"""Bedrock Data Automation service methods"""
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
