"""Tests for metrics service."""

import json

import pytest

from documentai_api.config.constants import MetricsGranularity
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
        "PayStub": 3332,
        "UnemploymentLetter": 91,
        "PaymentReceipt": 202,
        "Form1040": 140,
        "EmployerVerification": 164,
        "W2": 200,
        "LifeInsurance": 17,
        "BankStatement": 216,
        "AlimonySupport": 16,
        "SocialSecurityAward": 357,
        "ChildSupport": 55,
        "IRABalance": 26,
        "Resume": 6,
        "StateIncome": 15,
        "SocialSecurityCard": 1,
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
        "W2": 230,
        "PayStub": 4523,
        "null": 13472,
        "BankStatement": 291,
        "PaymentReceipt": 280,
        "UnemploymentLetter": 171,
        "SocialSecurityAward": 554,
        "StateIncome": 43,
        "Form1040": 239,
        "ChildSupport": 74,
        "EmployerVerification": 245,
        "IRABalance": 58,
        "AlimonySupport": 8,
        "Resume": 6,
        "SocialSecurityCard": 7,
        "LifeInsurance": 10,
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
        "total_processing_time_sum": 188524.18,
        "total_processing_time_count": 7207,
        "bda_processing_time_sum": 170475.24,
        "bda_processing_time_count": 7207,
        "bda_wait_time_sum": 18113.37,
        "bda_wait_time_count": 7208,
    },
}

STATS_MONTHLY_2026_01 = {
    "month": "2026-01",
    "total_records": 10000,
    "total_bda_invocations": 3500,
    "by_status": {
        "success": 2800,
        "not_implemented": 6200,
        "miscategorized": 180,
        "multipage": 45,
        "no_custom_blueprint_matched": 220,
        "password_protected": 95,
        "blurry_document_detected": 110,
        "failed": 340,
        "no_document_detected": 10,
    },
    "by_classification": {
        "PayStub": 1850,
        "null": 6720,
        "W2": 180,
        "BankStatement": 210,
        "PaymentReceipt": 150,
        "UnemploymentLetter": 95,
        "SocialSecurityAward": 320,
        "StateIncome": 25,
        "Form1040": 125,
        "ChildSupport": 48,
        "EmployerVerification": 140,
        "IRABalance": 35,
        "AlimonySupport": 12,
        "Resume": 8,
        "SocialSecurityCard": 3,
        "LifeInsurance": 9,
    },
    "by_response_code": {
        "000": 2450,
        "null": 6815,
        "102": 180,
        "002": 220,
        "101": 285,
        "104": 140,
        "999": 340,
        "103": 10,
    },
    "timing_stats": {
        "total_processing_time_sum": 95420.50,
        "total_processing_time_count": 3500,
        "bda_processing_time_sum": 88150.25,
        "bda_processing_time_count": 3500,
        "bda_wait_time_sum": 7280.15,
        "bda_wait_time_count": 3500,
    },
}

STATS_MONTHLY_2026_02 = {
    "month": "2026-02",
    "total_records": 34112,
    "total_bda_invocations": 12354,
    "by_status": {
        "success": 11050,
        "not_implemented": 20826,
        "miscategorized": 527,
        "multipage": 158,
        "no_custom_blueprint_matched": 769,
        "password_protected": 317,
        "blurry_document_detected": 345,
        "failed": 112,
        "started": 1,
        "no_document_detected": 7,
    },
    "by_classification": {
        "PayStub": 7855,
        "null": 22535,
        "W2": 430,
        "BankStatement": 507,
        "PaymentReceipt": 482,
        "UnemploymentLetter": 262,
        "SocialSecurityAward": 911,
        "StateIncome": 58,
        "Form1040": 379,
        "ChildSupport": 129,
        "EmployerVerification": 409,
        "IRABalance": 84,
        "AlimonySupport": 24,
        "Resume": 12,
        "SocialSecurityCard": 8,
        "LifeInsurance": 27,
    },
    "by_response_code": {
        "000": 9515,
        "null": 21647,
        "102": 527,
        "002": 769,
        "101": 971,
        "104": 564,
        "999": 112,
        "103": 7,
    },
    "timing_stats": {
        "total_processing_time_sum": 362808.66,
        "total_processing_time_count": 12353,
        "bda_processing_time_sum": 332087.02,
        "bda_processing_time_count": 12353,
        "bda_wait_time_sum": 30833.40,
        "bda_wait_time_count": 12354,
    },
}


def test_build_summary_single_day():
    """Test summary with one day of stats."""
    summary = _build_summary([STATS_2026_02_16])

    assert summary["total_records"] == 13901
    assert summary["total_bda_invocations"] == 5146
    assert summary["by_status"]["success"] == 4615
    assert summary["by_classification"]["PayStub"] == 3332


def test_build_summary_multiple_days():
    """Test summary across multiple days."""
    summary = _build_summary([STATS_2026_02_16, STATS_2026_02_17])

    assert summary["total_records"] == 34112  # 13901 + 20211
    assert summary["total_bda_invocations"] == 12354  # 5146 + 7208
    assert summary["by_status"]["success"] == 11050  # 4615 + 6435
    assert summary["by_classification"]["PayStub"] == 7855  # 3332 + 4523
    assert summary["by_response_code"]["000"] == 9515  # 4009 + 5506


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


@pytest.mark.parametrize(
    (
        "granularity",
        "start_date",
        "end_date",
        "s3_data",
        "expected_granularity",
        "expected_stats_key",
        "expected_count",
        "expected_total",
        "expected_timing_sum",
        "expected_timing_count",
    ),
    [
        # daily - single date
        (
            MetricsGranularity.DAILY,
            "2026-02-16",
            "2026-02-16",
            [("aggregated/date=2026-02-16/stats.json", "STATS_2026_02_16")],
            "daily",
            "daily_stats",
            1,
            13901,
            174284.48,
            5146,
        ),
        # daily - date range
        (
            MetricsGranularity.DAILY,
            "2026-02-16",
            "2026-02-17",
            [
                ("aggregated/date=2026-02-16/stats.json", "STATS_2026_02_16"),
                ("aggregated/date=2026-02-17/stats.json", "STATS_2026_02_17"),
            ],
            "daily",
            "daily_stats",
            2,
            34112,
            362808.66,  # 174284.48 + 188524.18
            12353,  # 5146 + 7207
        ),
        # daily - one day with data, one day without data
        (
            MetricsGranularity.DAILY,
            "2026-02-16",
            "2026-02-17",
            [("aggregated/date=2026-02-16/stats.json", "STATS_2026_02_16")],
            "daily",
            "daily_stats",
            1,
            13901,
            174284.48,
            5146,
        ),
        # monthly - single month
        (
            MetricsGranularity.MONTHLY,
            "2026-02-01",
            "2026-02-28",
            [("aggregated/month=2026-02/stats.json", "STATS_MONTHLY_2026_02")],
            "monthly",
            "monthly_stats",
            1,
            34112,
            362808.66,
            12353,
        ),
        # monthly - multiple months
        (
            MetricsGranularity.MONTHLY,
            "2026-01-15",
            "2026-02-15",
            [
                ("aggregated/month=2026-01/stats.json", "STATS_MONTHLY_2026_01"),
                ("aggregated/month=2026-02/stats.json", "STATS_MONTHLY_2026_02"),
            ],
            "monthly",
            "monthly_stats",
            2,
            44112,
            458229.16,  # 95420.50 + 362808.66
            15853,  # 3500 + 12353
        ),
        # monthly - one month with data, one month without data
        (
            MetricsGranularity.MONTHLY,
            "2026-01-01",
            "2026-02-28",
            [("aggregated/month=2026-02/stats.json", "STATS_MONTHLY_2026_02")],
            "monthly",
            "monthly_stats",
            1,
            34112,
            362808.66,
            12353,
        ),
    ],
)
def test_get_aggregated_metrics_granularity(
    s3_bucket,
    granularity,
    start_date,
    end_date,
    s3_data,
    expected_granularity,
    expected_stats_key,
    expected_count,
    expected_total,
    expected_timing_sum,
    expected_timing_count,
):
    """Test metrics with different granularities and scenarios."""
    stats_map = {
        "STATS_2026_02_16": STATS_2026_02_16,
        "STATS_2026_02_17": STATS_2026_02_17,
        "STATS_MONTHLY_2026_01": STATS_MONTHLY_2026_01,
        "STATS_MONTHLY_2026_02": STATS_MONTHLY_2026_02,
    }

    for s3_key, stats_name in s3_data:
        s3_bucket.put_object(
            Bucket="test-bucket",
            Key=s3_key,
            Body=json.dumps(stats_map[stats_name]),
        )

    result = get_aggregated_metrics("test-bucket", start_date, end_date, granularity)

    assert result["start_date"] == start_date
    assert result["end_date"] == end_date
    assert result["granularity"] == expected_granularity
    assert expected_stats_key in result
    assert len(result[expected_stats_key]) == expected_count
    assert result["summary"]["total_records"] == expected_total

    # cerify timing stats aggregation
    assert "timing_stats" in result["summary"]
    assert result["summary"]["timing_stats"]["total_processing_time_sum"] == pytest.approx(
        expected_timing_sum
    )
    assert result["summary"]["timing_stats"]["total_processing_time_count"] == expected_timing_count
