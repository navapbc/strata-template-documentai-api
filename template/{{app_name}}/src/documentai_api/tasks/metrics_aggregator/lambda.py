"""Lambda handler for daily metrics aggregator."""
from datetime import datetime, timedelta

from documentai_api.tasks.metrics_aggregator.main import main
from documentai_api.utils.error_handling import handle_lambda_errors
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


@handle_lambda_errors
def handler(event, context):
    """Lambda handler triggered by EventBridge schedule.

    Aggregates previous day's metrics data.

    Event can optionally include:
    - target_date: Date to aggregate (YYYY-MM-DD). Optional. Defaults to previous day.
    - overwrite: If True, re-aggregate even if stats exist.
    """
    target_date = event.get("target_date")

    if not target_date:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y-%m-%d")

    overwrite = event.get("overwrite", False)
    logger.info(f"Aggregating metrics for {target_date} (overwrite={overwrite})")
    result = main(target_date, overwrite)
    logger.info(f"Aggregation complete: {result}")

    return result
