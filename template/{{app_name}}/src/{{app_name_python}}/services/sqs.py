"""SQS Service methods"""

from typing import Any
from utils.aws_client_factory import AWSClientFactory


def send_message(queue_url: str, message_body: str, message_attributes: dict = None
) -> dict:
    """Send message to SQS"""
    sqs_client = AWSClientFactory.get_sqs_client()

    return sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body,
        MessageAttributes=message_attributes or {},
    )

def receive_messages(queue_url: str, max_messages: int = 10) -> list[dict[str, Any]]:
    """Receive messages from SQS queue"""
    sqs_client = AWSClientFactory.get_sqs_client()
    
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=20,
        VisibilityTimeout=300,
    )
    
    return response.get("Messages", [])

def delete_message(queue_url: str, receipt_handle: str) -> None:
    """Delete message from SQS queue"""
    sqs_client = AWSClientFactory.get_sqs_client()
    
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )
