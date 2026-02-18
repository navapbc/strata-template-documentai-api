"""Lambda handler for daily metrics aggregator."""

from documentai_api.tasks.metrics_aggregator.main import main
from documentai_api.utils.error_handling import handle_lambda_errors
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


@handle_lambda_errors
def handler(event, context):
    """Lambda handler triggered by EventBridge schedule.

    Aggregates previous day's metrics data.

    Event can optionally include:
    - target_date: Date to aggregate (YYYY-MM-DD). Required.
    - overwrite: If True, re-aggregate even if stats exist.
    """
    if "target_date" not in event:
        raise ValueError("target_date is required in event")

    target_date = event["target_date"]
    overwrite = event.get("overwrite", False)
    logger.info(f"Aggregating metrics for {target_date} (overwrite={overwrite})")
    result = main(target_date, overwrite)
    logger.info(f"Aggregation complete: {result}")

    return result
