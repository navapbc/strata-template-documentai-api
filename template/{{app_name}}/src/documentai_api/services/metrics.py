"""Metrics service for reading aggregated stats from S3."""

import json
from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def get_aggregated_metrics(bucket: str, start_date: str, end_date: str) -> dict:
    """Read aggregated metrics from S3 for date range."""
    s3 = AWSClientFactory.get_s3_client()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    daily_stats = []
    current_dt = start_dt

    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        s3_key = f"aggregated/date={date_str}/stats.json"

        try:
            obj = s3.get_object(Bucket=bucket, Key=s3_key)
            stats = json.loads(obj["Body"].read().decode())
            daily_stats.append(stats)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"No stats found for {date_str}")
            else:
                raise

        current_dt += timedelta(days=1)

    summary = _build_summary(daily_stats)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "daily_stats": daily_stats,
        "summary": summary,
    }


def _build_summary(daily_stats: list[dict]) -> dict:
    """Build summary stats across all dates."""
    summary = {
        "total_records": 0,
        "total_bda_invocations": 0,
        "by_status": {},
        "by_classification": {},
        "by_response_code": {},
    }

    for stats in daily_stats:
        summary["total_records"] += stats["total_records"]
        summary["total_bda_invocations"] += stats.get("total_bda_invocations", 0)

        for status, count in stats["by_status"].items():
            summary["by_status"][status] = summary["by_status"].get(status, 0) + count

        for classification, count in stats["by_classification"].items():
            summary["by_classification"][classification] = (
                summary["by_classification"].get(classification, 0) + count
            )

        for code, count in stats["by_response_code"].items():
            summary["by_response_code"][code] = summary["by_response_code"].get(code, 0) + count

    return summary
