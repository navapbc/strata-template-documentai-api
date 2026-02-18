#!/usr/bin/env python3
"""Process metrics from SQS queue and write to S3 for Athena queries.

Reads DDB records from SQS queue, writes them to S3 in partitioned structure,
and deletes processed messages from the queue.
"""

import json
import re
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.services import s3 as s3_service
from documentai_api.services import sqs as sqs_service
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def convert_keys_to_snake_case(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively convert all dictionary keys from camelCase to snake_case."""
    return {camel_to_snake(k): v for k, v in data.items()}


def write_to_s3(bucket_name: str, record: dict[str, Any]) -> None:
    """Write record to S3 in partitioned structure for Athena queries."""
    # convert keys to snake_case for athena
    record_as_snake_case = convert_keys_to_snake_case(record)

    # extract timestamp for partitioning
    created_at = record.get(DocumentMetadata.CREATED_AT, datetime.now(UTC).isoformat())
    dt = datetime.fromisoformat(created_at)

    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H")

    # generate unique filename
    file_id = str(uuid.uuid4())
    key = f"date={date_str}/hour={hour_str}/{file_id}.json"

    # write to S3
    s3_service.put_object(
        bucket_name, key, json.dumps(record_as_snake_case).encode(), "application/json"
    )

    logger.debug(f"Wrote record to s3://{bucket_name}/{key}")


def process_batch(
    queue_url: str,
    bucket_name: str,
    max_messages: int,
) -> int:
    """Process one batch of messages."""
    messages = sqs_service.receive_messages(queue_url, max_messages)

    if not messages:
        logger.info("No messages in queue")
        return 0

    logger.info(f"Processing {len(messages)} messages")

    processed = 0
    for message in messages:
        try:
            body = json.loads(message["Body"])
            write_to_s3(bucket_name, body)
            sqs_service.delete_message(queue_url, message["ReceiptHandle"])
            processed += 1
        except Exception as e:
            logger.error(f"Failed to process message: {e}", exc_info=True)

    logger.info(f"Successfully processed {processed}/{len(messages)} messages")
    return processed


def main(queue_url: str, bucket_name: str, max_messages: int = 10, max_batches: int = 10) -> int:
    """Process metrics from SQS queue and write to S3.

    Returns:
        Total number of messages processed.
    """
    logger.info("Starting metrics processor")
    logger.info(f"Queue URL: {queue_url}")
    logger.info(f"Bucket: {bucket_name}")

    total_processed = 0
    for batch_num in range(max_batches):
        logger.info(f"Processing batch {batch_num + 1}/{max_batches}")

        processed = process_batch(queue_url, bucket_name, max_messages)
        total_processed += processed

        if processed == 0:
            logger.info("Queue is empty, exiting")
            break

    logger.info(f"Finished processing {total_processed} total messages")
    return total_processed


if __name__ == "__main__":
    sys.exit(main())
