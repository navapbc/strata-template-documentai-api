"""Tests for metrics service."""

import json

from moto import mock_aws

from documentai_api.services.metrics import (
    _build_summary,
    get_aggregated_metrics,
)

STATS_2026_02_16 = {
    "date": "2026-02-16",
    "total_records": 13901,
    "total_bda_invocations": 5146,
    "by_status": {
        "not_implemented": 8378,
        "success": 4615,
        "blurry_document_detected": 124,
        "no_custom_blueprint_matched": 302,
        "miscategorized": 223,
        "password_protected": 128,
        "multipage": 66,
        "failed": 59,
        "no_document_detected": 6,
    },
    "by_classification": {
        "null": 9063,
        "custom-blueprint-pay-stub-v3": 3332,
        "custom-blueprint-unemployment-letter-v3": 91,
        "custom-blueprint-payment-receipt-v3": 202,
        "custom-blueprint-1040-v3": 140,
        "custom-blueprint-employer-verification-v3": 164,
        "custom-blueprint-w2-v3": 200,
        "custom-blueprint-life-insurance-v3": 17,
        "custom-blueprint-bank-statement-v3": 216,
        "custom-blueprint-alimony-support-v3": 16,
        "custom-blueprint-social-security-award-v3": 357,
        "custom-blueprint-child-support-v3": 55,
        "custom-blueprint-ira-balance-v3": 26,
        "custom-blueprint-resume-v3": 6,
        "custom-blueprint-state-income-v3": 15,
        "custom-blueprint-social-security-card-v3": 1,
    },
    "by_response_code": {
        "null": 8696,
        "000": 4009,
        "101": 395,
        "104": 211,
        "002": 302,
        "102": 223,
        "999": 59,
        "103": 6,
    },
    "by_hour": {
        "15": 1142,
        "6": 65,
        "21": 959,
        "22": 773,
        "23": 749,
        "13": 400,
        "3": 447,
        "20": 1134,
        "0": 623,
        "16": 1171,
        "1": 502,
        "17": 1210,
        "19": 1155,
        "14": 837,
        "2": 359,
        "18": 1326,
        "7": 88,
        "11": 59,
        "12": 166,
        "4": 393,
        "5": 215,
        "8": 42,
        "10": 27,
        "9": 59,
    },
    "timing_stats": {
        "total_processing_time_sum": 174284.48,
        "total_processing_time_count": 5146,
        "bda_processing_time_sum": 161611.78,
        "bda_processing_time_count": 5146,
        "bda_wait_time_sum": 12720.03,
        "bda_wait_time_count": 5146,
    },
}

STATS_2026_02_17 = {
    "date": "2026-02-17",
    "total_records": 20211,
    "total_bda_invocations": 7208,
    "by_status": {
        "success": 6435,
        "not_implemented": 12448,
        "miscategorized": 304,
        "multipage": 92,
        "no_custom_blueprint_matched": 467,
        "password_protected": 189,
        "blurry_document_detected": 221,
        "failed": 53,
        "started": 1,
        "no_document_detected": 1,
    },
    "by_classification": {
        "custom-blueprint-w2-v3": 230,
        "custom-blueprint-pay-stub-v3": 4523,
        "null": 13472,
        "custom-blueprint-bank-statement-v3": 291,
        "custom-blueprint-payment-receipt-v3": 280,
        "custom-blueprint-unemployment-letter-v3": 171,
        "custom-blueprint-social-security-award-v3": 554,
        "custom-blueprint-state-income-v3": 43,
        "custom-blueprint-1040-v3": 239,
        "custom-blueprint-child-support-v3": 74,
        "custom-blueprint-employer-verification-v3": 245,
        "custom-blueprint-ira-balance-v3": 58,
        "custom-blueprint-alimony-support-v3": 8,
        "custom-blueprint-resume-v3": 6,
        "custom-blueprint-social-security-card-v3": 7,
        "custom-blueprint-life-insurance-v3": 10,
    },
    "by_response_code": {
        "000": 5506,
        "null": 12951,
        "102": 304,
        "002": 467,
        "101": 576,
        "104": 353,
        "999": 53,
        "103": 1,
    },
    "by_hour": {
        "19": 1818,
        "13": 697,
        "22": 1254,
        "3": 517,
        "21": 1556,
        "20": 1695,
        "18": 1970,
        "2": 654,
        "15": 1701,
        "0": 692,
        "14": 1244,
        "17": 1765,
        "1": 732,
        "16": 1826,
        "5": 181,
        "7": 77,
        "23": 828,
        "4": 330,
        "12": 222,
        "9": 73,
        "11": 154,
        "6": 112,
        "10": 30,
        "8": 83,
    },
    "timing_stats": {
        "total_processing_time_sum": 188524.180000001,
        "total_processing_time_count": 7207,
        "bda_processing_time_sum": 170475.24,
        "bda_processing_time_count": 7207,
        "bda_wait_time_sum": 18113.37,
        "bda_wait_time_count": 7208,
    },
}


def test_build_summary_single_day():
    """Test summary with one day of stats."""
    summary = _build_summary([STATS_2026_02_16])

    assert summary["total_records"] == 13901
    assert summary["total_bda_invocations"] == 5146
    assert summary["by_status"]["success"] == 4615
    assert summary["by_classification"]["custom-blueprint-pay-stub-v3"] == 3332


def test_build_summary_multiple_days():
    """Test summary across multiple days."""
    summary = _build_summary([STATS_2026_02_16, STATS_2026_02_17])

    assert summary["total_records"] == 34112  # 13901 + 20211
    assert summary["total_bda_invocations"] == 12354  # 5146 + 7208
    assert summary["by_status"]["success"] == 11050  # 4615 + 6435
    assert summary["by_classification"]["custom-blueprint-pay-stub-v3"] == 7855  # 3332 + 4523
    assert summary["by_response_code"]["000"] == 9515  # 4009 + 5506


@mock_aws
def test_get_aggregated_metrics_single_date(s3_bucket):
    """Test fetching metrics for single date."""
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-16/stats.json",
        Body=json.dumps(STATS_2026_02_16),
    )

    result = get_aggregated_metrics("test-bucket", "2026-02-16", "2026-02-16")

    assert result["start_date"] == "2026-02-16"
    assert result["end_date"] == "2026-02-16"
    assert len(result["daily_stats"]) == 1
    assert result["daily_stats"][0]["total_records"] == 13901
    assert result["summary"]["total_records"] == 13901


@mock_aws
def test_get_aggregated_metrics_date_range(s3_bucket):
    """Test fetching metrics across date range."""
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-16/stats.json",
        Body=json.dumps(STATS_2026_02_16),
    )
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-17/stats.json",
        Body=json.dumps(STATS_2026_02_17),
    )

    result = get_aggregated_metrics("test-bucket", "2026-02-16", "2026-02-17")

    assert len(result["daily_stats"]) == 2
    assert result["summary"]["total_records"] == 34112
    assert result["summary"]["by_status"]["success"] == 11050


@mock_aws
def test_get_aggregated_metrics_missing_dates(s3_bucket):
    """Test handling missing dates in range."""
    s3_bucket.put_object(
        Bucket="test-bucket",
        Key="aggregated/date=2026-02-16/stats.json",
        Body=json.dumps(STATS_2026_02_16),
    )
    # 2026-02-17 missing

    result = get_aggregated_metrics("test-bucket", "2026-02-16", "2026-02-17")

    # should only have one day
    assert len(result["daily_stats"]) == 1
    assert result["summary"]["total_records"] == 13901
