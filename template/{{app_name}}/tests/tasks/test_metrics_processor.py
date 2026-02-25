"""Tests for metrics_processor."""

import json
from unittest.mock import patch

from moto import mock_aws

from documentai_api.tasks.metrics_processor.main import (
    camel_to_snake,
    convert_keys_to_snake_case,
    main,
    process_batch,
    write_to_s3,
)


def test_camel_to_snake():
    """Test camelCase to snake_case conversion."""
    assert camel_to_snake("fileName") == "file_name"
    assert camel_to_snake("processStatus") == "process_status"
    assert camel_to_snake("bdaInvocationArn") == "bda_invocation_arn"
    assert camel_to_snake("createdAt") == "created_at"
    assert camel_to_snake("alreadySnake") == "already_snake"


def test_convert_keys_to_snake_case():
    """Test dictionary key conversion."""
    data = {
        "fileName": "test.pdf",
        "processStatus": "success",
        "createdAt": "2026-02-20T10:00:00Z",
    }

    result = convert_keys_to_snake_case(data)

    assert result["file_name"] == "test.pdf"
    assert result["process_status"] == "success"
    assert result["created_at"] == "2026-02-20T10:00:00Z"


def test_write_to_s3(s3_client, s3_bucket):
    """Test writing record to S3 with partitioning."""
    record = {
        "fileName": "test.pdf",
        "processStatus": "success",
        "createdAt": "2026-02-20T10:30:00Z",
    }

    write_to_s3("test-bucket", record)

    # verify file was written with correct partitioning
    objects = s3_client.list_objects_v2(Bucket="test-bucket", Prefix="date=2026-02-20/hour=10/")
    assert objects["KeyCount"] == 1

    # verify content
    key = objects["Contents"][0]["Key"]
    obj = s3_client.get_object(Bucket="test-bucket", Key=key)
    content = json.loads(obj["Body"].read().decode())

    assert content["file_name"] == "test.pdf"
    assert content["process_status"] == "success"


def test_write_to_s3_default_timestamp(s3_client, s3_bucket):
    """Test writing record without createdAt uses current time."""
    record = {
        "fileName": "test.pdf",
        "processStatus": "success",
    }

    write_to_s3("test-bucket", record)

    # should create partition with current date
    objects = s3_client.list_objects_v2(Bucket="test-bucket")
    assert objects["KeyCount"] == 1


def test_process_batch_empty_queue():
    """Test processing when queue is empty."""
    with patch(
        "documentai_api.tasks.metrics_processor.main.sqs_service.receive_messages"
    ) as mock_receive:
        mock_receive.return_value = []

        result = process_batch("queue-url", "bucket-name", 10)

        assert result == 0


def test_process_batch_success(s3_client, s3_bucket):
    """Test successful batch processing."""
    messages = [
        {
            "Body": json.dumps(
                {
                    "fileName": "test1.pdf",
                    "processStatus": "success",
                    "createdAt": "2026-02-20T10:00:00Z",
                }
            ),
            "ReceiptHandle": "handle-1",
        },
        {
            "Body": json.dumps(
                {
                    "fileName": "test2.pdf",
                    "processStatus": "failed",
                    "createdAt": "2026-02-20T11:00:00Z",
                }
            ),
            "ReceiptHandle": "handle-2",
        },
    ]

    with (
        patch(
            "documentai_api.tasks.metrics_processor.main.sqs_service.receive_messages"
        ) as mock_receive,
        patch(
            "documentai_api.tasks.metrics_processor.main.sqs_service.delete_message"
        ) as mock_delete,
    ):
        mock_receive.return_value = messages

        result = process_batch("queue-url", "test-bucket", 10)

        assert result == 2
        assert mock_delete.call_count == 2

        # verify both files were written
        objects = s3_client.list_objects_v2(Bucket="test-bucket")
        assert objects["KeyCount"] == 2


def test_process_batch_partial_failure(s3_client, s3_bucket):
    """Test batch processing with one message failing."""
    messages = [
        {
            "Body": json.dumps({"fileName": "test1.pdf", "createdAt": "2026-02-20T10:00:00Z"}),
            "ReceiptHandle": "handle-1",
        },
        {"Body": "invalid json", "ReceiptHandle": "handle-2"},
    ]

    with (
        patch(
            "documentai_api.tasks.metrics_processor.main.sqs_service.receive_messages"
        ) as mock_receive,
        patch(
            "documentai_api.tasks.metrics_processor.main.sqs_service.delete_message"
        ) as mock_delete,
    ):
        mock_receive.return_value = messages

        result = process_batch("queue-url", "test-bucket", 10)

        # only one message processed successfully
        assert result == 1
        assert mock_delete.call_count == 1


def test_main_processes_multiple_batches(s3_client, s3_bucket):
    """Test main processes multiple batches until queue is empty."""
    batch_1 = [
        {
            "Body": json.dumps({"fileName": "test1.pdf", "createdAt": "2026-02-20T10:00:00Z"}),
            "ReceiptHandle": "h1",
        }
    ]
    batch_2 = [
        {
            "Body": json.dumps({"fileName": "test2.pdf", "createdAt": "2026-02-20T11:00:00Z"}),
            "ReceiptHandle": "h2",
        }
    ]

    with (
        patch(
            "documentai_api.tasks.metrics_processor.main.sqs_service.receive_messages"
        ) as mock_receive,
        patch("documentai_api.tasks.metrics_processor.main.sqs_service.delete_message"),
    ):
        # return messages for first two batches, then empty
        mock_receive.side_effect = [batch_1, batch_2, []]

        result = main("queue-url", "test-bucket", max_messages=10, max_batches=5)

        assert result == 2  # total messages processed
        assert mock_receive.call_count == 3  # called 3 times (stopped when empty)


def test_main_respects_max_batches(s3_client, s3_bucket):
    """Test main stops after max_batches even if queue has more."""
    messages = [
        {
            "Body": json.dumps({"fileName": "test.pdf", "createdAt": "2026-02-20T10:00:00Z"}),
            "ReceiptHandle": "h1",
        }
    ]

    with (
        patch(
            "documentai_api.tasks.metrics_processor.main.sqs_service.receive_messages"
        ) as mock_receive,
        patch("documentai_api.tasks.metrics_processor.main.sqs_service.delete_message"),
    ):
        # always return messages (queue never empty)
        mock_receive.return_value = messages

        result = main("queue-url", "test-bucket", max_messages=10, max_batches=3)

        assert result == 3  # processed 3 batches
        assert mock_receive.call_count == 3  # stopped at max_batches
