"""Tests for metrics_aggregator."""

from unittest.mock import patch

import pytest
from moto import mock_aws

from documentai_api.tasks.metrics_aggregator.main import (
    _aggregate_records,
    _build_deduplication_query,
    _check_if_previously_aggregated,
    _write_aggregated_stats,
    main,
)
from documentai_api.utils import env

def create_record(
    status="success",
    created_at="2026-02-20T10:00:00Z",
    blueprint_name="W2",
    total_time=None,
    bda_time=None,
):
    """Factory function to create test records with defaults."""
    record = {
        "file_name": "test.pdf",
        "process_status": status,
        "created_at": created_at,
        "bda_matched_blueprint_name": blueprint_name,
    }
    if total_time:
        record["total_processing_time_seconds"] = str(total_time)
    if bda_time:
        record["bda_processing_time_seconds"] = str(bda_time)
    return record


def test_build_deduplication_query():
    """Test SQL query generation."""
    query = _build_deduplication_query("test_db", "test_table", "2026-02-20")

    assert "test_db.test_table" in query
    assert "created_at = '2026-02-20'" in query
    assert "ROW_NUMBER() OVER" in query
    assert "PARTITION BY file_name" in query
    assert "ORDER BY updated_at DESC" in query
    assert "WHERE rn = 1" in query


def test_aggregate_records_empty():
    """Test aggregation with no records."""
    stats = _aggregate_records([], "2026-02-20")

    assert stats["date"] == "2026-02-20"
    assert stats["total_records"] == 0
    assert stats["by_status"] == {}
    assert stats["by_hour"] == {}
    assert stats["by_classification"] == {}
    assert stats["by_response_code"] == {}
    assert stats["timing_stats"] == {
        "total_processing_time_sum": 0,
        "total_processing_time_count": 0,
        "bda_processing_time_sum": 0,
        "bda_processing_time_count": 0,
        "bda_wait_time_sum": 0,
        "bda_wait_time_count": 0,
    }


def test_aggregate_records_single_record():
    """Test aggregation with one record."""
    record = create_record(
        status="success",
        created_at="2026-02-20T10:30:00Z",
        blueprint_name="W2",
        total_time=5.5,
        bda_time=3.2,
    )

    stats = _aggregate_records([record], "2026-02-20")

    assert stats["total_records"] == 1
    assert stats["by_status"]["success"] == 1
    assert stats["by_hour"]["10"] == 1
    assert stats["by_classification"]["W2"] == 1
    assert stats["timing_stats"]["total_processing_time_sum"] == 5.5
    assert stats["timing_stats"]["total_processing_time_count"] == 1
    assert stats["timing_stats"]["bda_processing_time_sum"] == 3.2
    assert stats["timing_stats"]["bda_processing_time_count"] == 1


def test_aggregate_records_multiple_records():
    """Test aggregation with multiple records."""
    records = [
        create_record(status="success", created_at="2026-02-20T10:00:00Z", total_time=5.0),
        create_record(status="success", created_at="2026-02-20T10:30:00Z", total_time=3.0),
        create_record(status="failed", created_at="2026-02-20T11:00:00Z", blueprint_name="1099"),
    ]

    stats = _aggregate_records(records, "2026-02-20")

    assert stats["total_records"] == 3
    assert stats["by_status"]["success"] == 2
    assert stats["by_status"]["failed"] == 1
    assert stats["by_hour"]["10"] == 2
    assert stats["by_hour"]["11"] == 1
    assert stats["by_classification"]["W2"] == 2
    assert stats["by_classification"]["1099"] == 1
    assert stats["timing_stats"]["total_processing_time_sum"] == 8.0
    assert stats["timing_stats"]["total_processing_time_count"] == 2


@mock_aws
def test_check_if_previously_aggregated(s3_client, s3_bucket):
    """Test checking for existing aggregation."""
    # create the aggregated stats file
    s3_client.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-20/stats.json",
        Body=b'{"date": "2026-02-20"}',
    )

    result = _check_if_previously_aggregated("test-bucket", "2026-02-20")

    assert result is True


@mock_aws
def test_check_if_previously_aggregated_not_exists(s3_client, s3_bucket):
    """Test checking for non-existent aggregation."""
    result = _check_if_previously_aggregated("test-bucket", "2026-02-20")

    assert result is False


@mock_aws
def test_write_aggregated_stats(s3_client, s3_bucket):
    """Test writing aggregated stats to S3."""
    stats = {"date": "2026-02-20", "total_records": 10, "by_status": {"success": 8, "failed": 2}}

    s3_key = _write_aggregated_stats("test-bucket", stats, "2026-02-20")

    assert s3_key == "aggregated/date=2026-02-20/stats.json"

    # verify file was written
    obj = s3_client.get_object(Bucket="test-bucket", Key=s3_key)
    content = obj["Body"].read().decode()
    assert "2026-02-20" in content
    assert "total_records" in content


@mock_aws
def test_main_already_aggregated(s3_client, s3_bucket):
    """Test main skips when already aggregated."""
    # create existing aggregation
    s3_client.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-20/stats.json",
        Body=b'{"date": "2026-02-20"}',
    )

    with patch.dict("os.environ", {env.DOCUMENTAI_METRICS_BUCKET_NAME: "test-bucket"}):
        result = main("2026-02-20", overwrite=False)

    assert result["statusCode"] == 200
    assert result["message"] == "Already aggregated"
    assert "2026-02-20" in result["outputLocation"]


@mock_aws
@pytest.mark.parametrize(
    ("overwrite", "should_skip"),
    [
        (False, True),  # normal run - skips aggregation if exists
        (True, False),  # overwrite - processes aggregation even if exists
    ],
)
def test_main_success(s3_client, s3_bucket, overwrite, should_skip):
    """Test successful aggregation with and without overwrite."""
    # create existing aggregation
    s3_client.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-20/stats.json",
        Body=b'{"date": "2026-02-20"}',
    )

    with (
        patch("documentai_api.tasks.metrics_aggregator.main._execute_athena_query") as mock_athena,
        patch("documentai_api.tasks.metrics_aggregator.main._get_athena_results") as mock_results,
        patch.dict(
            "os.environ",
            {
                env.DOCUMENTAI_GLUE_DATABASE_NAME: "test_db",
                env.DOCUMENTAI_METRICS_RAW_TABLE_NAME: "test_table",
                env.DOCUMENTAI_ATHENA_RESULTS_BUCKET_NAME: "athena-bucket",
                env.DOCUMENTAI_METRICS_BUCKET_NAME: "test-bucket",
            },
        ),
    ):
        mock_athena.return_value = "execution-123"
        mock_results.return_value = [
            {"process_status": "success", "created_at": "2026-02-20T10:00:00Z"}
        ]

        result = main("2026-02-20", overwrite=overwrite)

        if should_skip:
            # should skip and not call athena
            mock_athena.assert_not_called()
            assert result["message"] == "Already aggregated"
        else:
            # should process and call athena
            mock_athena.assert_called_once()
            assert result["recordsProcessed"] == 1

        assert result["statusCode"] == 200
        assert result["date"] == "2026-02-20"
