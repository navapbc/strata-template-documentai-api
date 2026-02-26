"""Metrics service for reading aggregated stats from S3."""

import json
from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from documentai_api.config.constants import MetricsGranularity
from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def get_aggregated_metrics(
    bucket: str,
    start_date: str,
    end_date: str,
    granularity: MetricsGranularity = MetricsGranularity.DAILY,
) -> dict:
    """Read aggregated metrics from S3 for date range.

    Args:
        bucket: S3 bucket name
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        granularity: Aggregation granularity (MetricsGranularity.DAILY or MetricsGranularity.MONTHLY)
    """
    if granularity == MetricsGranularity.MONTHLY:
        return _get_monthly_metrics(bucket, start_date, end_date)
    else:
        return _get_daily_metrics(bucket, start_date, end_date)


def _get_daily_metrics(bucket: str, start_date: str, end_date: str) -> dict:
    """Read daily aggregated metrics from S3."""
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
        "granularity": MetricsGranularity.DAILY.value,
        "daily_stats": daily_stats,
        "summary": summary,
    }


def _get_monthly_metrics(bucket: str, start_date: str, end_date: str) -> dict:
    """Read monthly aggregated metrics from S3."""
    s3 = AWSClientFactory.get_s3_client()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # generate list of unique months in range
    months = set()
    current_dt = start_dt
    while current_dt <= end_dt:
        months.add(current_dt.strftime("%Y-%m"))
        # move to next month
        if current_dt.month == 12:
            current_dt = current_dt.replace(year=current_dt.year + 1, month=1, day=1)
        else:
            current_dt = current_dt.replace(month=current_dt.month + 1, day=1)

    monthly_stats = []
    for yyyymm in sorted(months):
        s3_key = f"aggregated/month={yyyymm}/stats.json"

        try:
            obj = s3.get_object(Bucket=bucket, Key=s3_key)
            stats = json.loads(obj["Body"].read().decode())
            monthly_stats.append(stats)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"No stats found for {yyyymm}")
            else:
                raise

    summary = _build_summary(monthly_stats)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "granularity": MetricsGranularity.MONTHLY.value,
        "monthly_stats": monthly_stats,
        "summary": summary,
    }


def _build_summary(stats_list: list[dict]) -> dict:
    """Build summary stats across all periods."""
    summary = {
        "total_records": 0,
        "total_bda_invocations": 0,
        "by_status": {},
        "by_classification": {},
        "by_response_code": {},
        "timing_stats": {
            "total_processing_time_sum": 0,
            "total_processing_time_count": 0,
            "bda_processing_time_sum": 0,
            "bda_processing_time_count": 0,
            "bda_wait_time_sum": 0,
            "bda_wait_time_count": 0,
        },
    }

    for stats in stats_list:
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

        # aggregate timing stats if present
        if "timing_stats" in stats:
            timing = stats["timing_stats"]
            summary["timing_stats"]["total_processing_time_sum"] += timing.get(
                "total_processing_time_sum", 0
            )
            summary["timing_stats"]["total_processing_time_count"] += timing.get(
                "total_processing_time_count", 0
            )
            summary["timing_stats"]["bda_processing_time_sum"] += timing.get(
                "bda_processing_time_sum", 0
            )
            summary["timing_stats"]["bda_processing_time_count"] += timing.get(
                "bda_processing_time_count", 0
            )
            summary["timing_stats"]["bda_wait_time_sum"] += timing.get("bda_wait_time_sum", 0)
            summary["timing_stats"]["bda_wait_time_count"] += timing.get("bda_wait_time_count", 0)

    return summary
