"""Lambda handler for metrics processor."""

import json
import logging
import os

from documentai_api.tasks.metrics_processor.main import process_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    Lambda handler triggered by EventBridge schedule.
    Processes metrics from SQS queue and writes to S3.
    """
    queue_url = os.environ["DDE_METRICS_QUEUE_URL"]
    bucket_name = os.environ["DDE_METRICS_BUCKET_NAME"]
    max_messages = int(os.environ.get("DDE_METRICS_QUEUE_MAX_MESSAGES", "10"))
    max_batches = int(os.environ.get("DDE_METRICS_QUEUE_MAX_BATCHES", "10"))

    logger.info(f"Processing metrics from queue: {queue_url}")

    total_processed = 0
    for batch_num in range(max_batches):
        logger.info(f"Processing batch {batch_num + 1}/{max_batches}")

        processed = process_batch(queue_url, bucket_name, max_messages, logger)
        total_processed += processed

        if processed == 0:
            logger.info("Queue is empty, exiting")
            break

    logger.info(f"Finished processing {total_processed} total messages")

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": total_processed}),
    }
