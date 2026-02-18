import json
import pytest

from fastapi.testclient import TestClient
from moto import mock_aws
from unittest.mock import patch
from documentai_api.app import app

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"message": "healthy"}


def test_config():
    response = client.get("/config")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "supportedFileTypes" in data


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "status" in response.json()


def test_document_upload_no_file():
    response = client.post("/v1/documents")
    assert response.status_code == 422  # Missing required file


def test_document_status_not_found():
    # this endpoint requires aws infrastructure in order to work properly
    # for now, just verify it responds
    # TODO: add moto to mock aws services
    response = client.get("/v1/documents/fake-job-id")
    assert response.status_code in [404, 500]


@mock_aws
@pytest.mark.parametrize(
    ("query_params", "expected_start", "expected_end", "expected_daily_count", "expected_total"),
    [
        ("start_date=2026-02-16&end_date=2026-02-17", "2026-02-16", "2026-02-17", 2, 34112),
        ("start_date=2026-02-16&end_date=2026-02-18", "2026-02-16", "2026-02-18", 2, 34112),
        ("start_date=2026-02-16", "2026-02-16", "2026-03-18", 2, 34112),
        ("start_date=2026-01-01&end_date=2026-01-02", "2026-01-01", "2026-01-02", 0, 0),  # no data
    ],
)
def test_metrics_endpoint_success(s3_bucket, query_params, expected_start, expected_end, expected_daily_count, expected_total):
    """Test metrics endpoint with various date ranges."""
    from tests.services.test_metrics import STATS_2026_02_16, STATS_2026_02_17
    
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
    
    with patch.dict("os.environ", {"DDE_METRICS_BUCKET_NAME": "test-bucket"}):
        response = client.get(f"/v1/metrics/?{query_params}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["start_date"] == expected_start
    assert data["end_date"] == expected_end
    assert len(data["daily_stats"]) == expected_daily_count
    assert data["summary"]["total_records"] == expected_total


def test_metrics_endpoint_missing_start_date():
    """Test metrics endpoint requires start_date."""
    response = client.get("/v1/metrics/")
    assert response.status_code == 422


def test_metrics_endpoint_invalid_date_format():
    """Test metrics endpoint validates date format."""
    response = client.get("/v1/metrics/?start_date=2026/02/16")
    assert response.status_code == 400


def test_metrics_endpoint_start_after_end():
    """Test metrics endpoint rejects start_date after end_date."""
    response = client.get("/v1/metrics/?start_date=2026-02-18&end_date=2026-02-16")
    assert response.status_code == 400
    assert "start_date must be before or equal to end_date" in response.json()["detail"]


def test_metrics_endpoint_missing_bucket_config():
    """Test metrics endpoint handles missing bucket configuration."""
    with patch.dict("os.environ", {"DDE_METRICS_BUCKET_NAME": ""}, clear=True):
        response = client.get("/v1/metrics/?start_date=2026-02-16")
    
    assert response.status_code == 500
    assert "Metrics bucket not configured" in response.json()["detail"]


@mock_aws
def test_metrics_endpoint_service_error(s3_bucket):
    """Test metrics endpoint handles service errors."""
    with (
        patch.dict("os.environ", {"DDE_METRICS_BUCKET_NAME": "test-bucket"}),
        patch("documentai_api.services.metrics.get_aggregated_metrics", side_effect=Exception("S3 error")),
    ):
        response = client.get("/v1/metrics/?start_date=2026-02-16")
    
    assert response.status_code == 500
    assert "Failed to retrieve metrics" in response.json()["detail"]
