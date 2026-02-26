"""Daily metrics aggregation job.

Aggregates previous day's metrics data from S3 via Athena queries.
Writes aggregated stats to S3 for historical analysis.
"""

import json
import os
import time
from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from documentai_api.utils import env
from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.dates import validate_yyyymmdd_format
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def _build_deduplication_query(database_name: str, table_name: str, target_date: str) -> str:
    """Build Athena query to deduplicate records for the target date."""
    # use row_number window function to get latest record per file_name, in the
    # off chance there are duplicate record by file name and created_at. this is
    # a safeguard to ensure we don't double count records in the aggregation if
    # duplicates exist.
    #
    # note: do not cast created_at to a string. it will prevent athena from partition
    # pruning and cause full table scan, which can lead to timeouts and higher costs
    query = f"""
    WITH ranked_records AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY file_name
                ORDER BY updated_at DESC
            ) AS rn
        FROM {database_name}.{table_name}
        WHERE date = '{target_date}'
    )
    SELECT *
    FROM ranked_records
    WHERE rn = 1
    """
    return query


def _aggregate_records(records: list[dict], target_date: str) -> dict:
    """Aggregate records into stats. Pure function for easy testing."""
    stats = _initialize_stats(target_date)

    for record in records:
        _process_record(record, stats)

    if stats["timing_stats"]["total_processing_time_count"] > 0:
        stats["timing_stats"]["total_processing_time_avg"] = round(
            stats["timing_stats"]["total_processing_time_sum"]
            / stats["timing_stats"]["total_processing_time_count"],
            2,
        )

    if stats["timing_stats"]["bda_processing_time_count"] > 0:
        stats["timing_stats"]["bda_processing_time_avg"] = round(
            stats["timing_stats"]["bda_processing_time_sum"]
            / stats["timing_stats"]["bda_processing_time_count"],
            2,
        )

    if stats["timing_stats"]["bda_wait_time_count"] > 0:
        stats["timing_stats"]["bda_wait_time_avg"] = round(
            stats["timing_stats"]["bda_wait_time_sum"]
            / stats["timing_stats"]["bda_wait_time_count"],
            2,
        )

    return stats


def _execute_athena_query(query: str, database_name: str, workgroup_name: str) -> str:
    """Execute Athena query and return execution ID."""
    athena = AWSClientFactory.get_athena_client()

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_name},
        WorkGroup=workgroup_name,
    )

    return response["QueryExecutionId"]


def _get_athena_results(execution_id: str) -> list[dict]:
    """Get results from completed Athena query."""
    athena = AWSClientFactory.get_athena_client()

    # wait for query completion
    while True:
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        status = response["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if status != "SUCCEEDED":
        raise Exception(f"Query failed with status: {status}")

    # get results
    results = []
    paginator = athena.get_paginator("get_query_results")

    for page in paginator.paginate(QueryExecutionId=execution_id):
        for row in page["ResultSet"]["Rows"][1:]:  # Skip header
            record = {}
            for i, col in enumerate(page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]):
                record[col["Name"]] = row["Data"][i].get("VarCharValue", "")
            results.append(record)

    return results


def _check_if_previously_aggregated(bucket: str, target_date: str) -> bool:
    """Check if stats already exist for the given date."""
    s3 = AWSClientFactory.get_s3_client()
    s3_key = f"aggregated/date={target_date}/stats.json"

    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError:
        return False


def _initialize_stats(target_date: str) -> dict:
    """Initialize empty stats structure."""
    return {
        "date": target_date,
        "total_records": 0,
        "total_bda_invocations": 0,
        "by_status": {},
        "by_classification": {},
        "by_response_code": {},
        "by_hour": {},
        "timing_stats": {
            "total_processing_time_avg": 0,
            "total_processing_time_sum": 0,
            "total_processing_time_count": 0,
            "bda_processing_time_avg": 0,
            "bda_processing_time_sum": 0,
            "bda_processing_time_count": 0,
            "bda_wait_time_avg": 0,
            "bda_wait_time_sum": 0,
            "bda_wait_time_count": 0,
        },
    }


def _process_record(record: dict, stats: dict):
    """Process a single record into aggregation stats."""
    stats["total_records"] += 1

    # count by status
    status = record.get("process_status", "unknown")
    stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

    # count by classification (blueprint name)
    classification = record.get("bda_matched_document_class") or "null"
    stats["by_classification"][classification] = (
        stats["by_classification"].get(classification, 0) + 1
    )

    # count by response code
    response_code = record.get("response_code") or "null"
    stats["by_response_code"][response_code] = stats["by_response_code"].get(response_code, 0) + 1

    # count BDA invocations
    if record.get("bda_invocation_arn"):
        stats["total_bda_invocations"] += 1

    # count by hour
    created_at = record.get("created_at")
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at)
            hour = str(dt.hour)  # store as string to match production
            stats["by_hour"][hour] = stats["by_hour"].get(hour, 0) + 1
        except (ValueError, TypeError):
            pass

    # timing stats
    total_time = record.get("total_processing_time_seconds")
    if total_time:
        try:
            stats["timing_stats"]["total_processing_time_sum"] += float(total_time)
            stats["timing_stats"]["total_processing_time_count"] += 1
        except (ValueError, TypeError):
            pass

    bda_time = record.get("bda_processing_time_seconds")
    if bda_time:
        try:
            stats["timing_stats"]["bda_processing_time_sum"] += float(bda_time)
            stats["timing_stats"]["bda_processing_time_count"] += 1
        except (ValueError, TypeError):
            pass

    bda_wait = record.get("bda_wait_time_seconds")
    if bda_wait:
        try:
            stats["timing_stats"]["bda_wait_time_sum"] += float(bda_wait)
            stats["timing_stats"]["bda_wait_time_count"] += 1
        except (ValueError, TypeError):
            pass


def _write_aggregated_stats(bucket: str, stats: dict, target_date: str):
    """Write aggregated stats to S3."""
    s3 = AWSClientFactory.get_s3_client()

    s3_key = f"aggregated/date={target_date}/stats.json"

    # round sums to avoid floating point precision issues
    stats["timing_stats"]["total_processing_time_sum"] = round(
        stats["timing_stats"]["total_processing_time_sum"], 2
    )
    stats["timing_stats"]["bda_processing_time_sum"] = round(
        stats["timing_stats"]["bda_processing_time_sum"], 2
    )
    stats["timing_stats"]["bda_wait_time_sum"] = round(
        stats["timing_stats"]["bda_wait_time_sum"], 2
    )

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(stats, default=str),
        ContentType="application/json",
    )

    logger.info(f"Aggregated stats written to s3://{bucket}/{s3_key}")
    return s3_key


def _get_daily_stats_for_month(bucket: str, yyyymm: str) -> list[dict]:
    """Read all daily stats for a given month from S3."""
    s3 = AWSClientFactory.get_s3_client()
    prefix = f"aggregated/date={yyyymm}-"

    daily_stats = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("stats.json"):
                response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                daily_stats.append(json.loads(response["Body"].read().decode()))

    return daily_stats


def _aggregate_monthly(bucket: str, yyyymm: str) -> dict | None:
    """Aggregate daily stats into monthly stats."""
    s3_key = f"aggregated/month={yyyymm}/stats.json"
    s3 = AWSClientFactory.get_s3_client()

    daily_stats = _get_daily_stats_for_month(bucket, yyyymm)
    if not daily_stats:
        logger.warning(f"No daily stats found for {yyyymm}")
        return None

    from documentai_api.services.metrics import _build_summary

    monthly_stats = _build_summary(daily_stats)
    monthly_stats["month"] = yyyymm

    s3.put_object(
        Bucket=bucket, Key=s3_key, Body=json.dumps(monthly_stats), ContentType="application/json"
    )
    logger.info(f"Monthly stats written to s3://{bucket}/{s3_key}")
    return {
        "month": yyyymm,
        "outputLocation": f"s3://{bucket}/{s3_key}",
        "daysProcessed": len(daily_stats),
    }


def main(target_date: str, overwrite: bool = False) -> dict:
    """Aggregate metrics for a specific date.

    Args:
        target_date: Date to aggregate in YYYY-MM-DD format (required).

    Returns:
        Dict with statusCode, date, recordsProcessed, and outputLocation.
    """
    # validate YYYY-MM-DD format, will raise ValueError if invalid
    validate_yyyymmdd_format(target_date)
    yyyymm = target_date[:7]

    database_name = os.getenv(env.DOCUMENTAI_GLUE_DATABASE_NAME)
    table_name = os.getenv(env.DOCUMENTAI_METRICS_RAW_TABLE_NAME)
    workgroup_name = os.getenv(env.DOCUMENTAI_ATHENA_WORKGROUP_NAME)
    metrics_bucket = os.getenv(env.DOCUMENTAI_METRICS_BUCKET_NAME)

    if not overwrite and _check_if_previously_aggregated(metrics_bucket, target_date):
        s3_key = f"aggregated/date={target_date}/stats.json"
        logger.info(f"Stats for {target_date} already exist, skipping")
        return {
            "statusCode": 200,
            "date": target_date,
            "message": "Already aggregated",
            "outputLocation": f"s3://{metrics_bucket}/{s3_key}",
        }

    query = _build_deduplication_query(database_name, table_name, target_date)
    execution_id = _execute_athena_query(query, database_name, workgroup_name)
    records = _get_athena_results(execution_id)
    stats = _aggregate_records(records, target_date)
    s3_key = _write_aggregated_stats(metrics_bucket, stats, target_date)

    # aggregate current month
    monthly_results = []
    monthly_result = _aggregate_monthly(metrics_bucket, yyyymm)
    if monthly_result:
        monthly_results.append(monthly_result)

    # if first day of month, finalize previous month
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    if dt.day == 1:
        yyyymm_previous = (dt - timedelta(days=1)).strftime("%Y-%m")
        prev_result = _aggregate_monthly(metrics_bucket, yyyymm_previous)

        if prev_result:
            monthly_results.append(prev_result)

    return {
        "statusCode": 200,
        "date": target_date,
        "recordsProcessed": stats["total_records"],
        "outputLocation": f"s3://{metrics_bucket}/{s3_key}",
        "monthlyAggregations": monthly_results,
    }
