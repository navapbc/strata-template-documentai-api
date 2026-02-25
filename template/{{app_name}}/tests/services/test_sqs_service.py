"""Tests for SQS Service methods."""

from moto import mock_aws

from documentai_api.services import sqs as sqs_service


@mock_aws
def test_send_message():
    """Test sending message to SQS."""
    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue["QueueUrl"]

    response = sqs_service.send_message(queue_url, "test message")

    assert "MessageId" in response
    messages = sqs.receive_message(QueueUrl=queue_url)
    assert messages["Messages"][0]["Body"] == "test message"


@mock_aws
def test_receive_messages():
    """Test receiving messages from SQS."""
    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue["QueueUrl"]
    sqs.send_message(QueueUrl=queue_url, MessageBody="test message")

    messages = sqs_service.receive_messages(queue_url)

    assert len(messages) == 1
    assert messages[0]["Body"] == "test message"


@mock_aws
def test_receive_messages_empty_queue():
    """Test receiving from empty queue."""
    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue["QueueUrl"]

    messages = sqs_service.receive_messages(queue_url)

    assert messages == []


@mock_aws
def test_delete_message():
    """Test deleting message from SQS."""
    import boto3

    sqs = boto3.client("sqs", region_name="us-east-1")
    queue = sqs.create_queue(QueueName="test-queue")
    queue_url = queue["QueueUrl"]
    sqs.send_message(QueueUrl=queue_url, MessageBody="test message")

    messages = sqs.receive_message(QueueUrl=queue_url)
    receipt_handle = messages["Messages"][0]["ReceiptHandle"]

    sqs_service.delete_message(queue_url, receipt_handle)

    # verify message was deleted
    messages = sqs.receive_message(QueueUrl=queue_url)
    assert "Messages" not in messages
