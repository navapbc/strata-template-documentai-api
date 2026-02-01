import os
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from schemas.document_metadata import DocumentMetadata
from utils import ddb as ddb_util


@pytest.fixture
def mock_s3_service():
    """Mock s3_service"""
    with patch("utils.ddb.s3_service") as mock:
        yield mock


@pytest.fixture
def mock_ddb_service():
    """Mock ddb_service"""
    with patch("utils.ddb.ddb_service") as mock:
        yield mock


@pytest.mark.parametrize(
    "arn,expected_region",
    [
        ("arn:aws:bedrock-data-automation:us-east-1:123456789012:job/abc123", "us-east-1"),
        ("arn:aws:bedrock-data-automation:eu-west-1:123456789012:job/xyz789", "eu-west-1"),
        ("invalid-arn", None),
    ],
)
def test_extract_region_from_bda_arn(arn, expected_region):
    """Test extracting AWS region from BDA ARN"""
    assert ddb_util.extract_region_from_bda_arn(arn) == expected_region


def test_get_elapsed_time_seconds():
    """Test elapsed time calculation"""
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 12, 0, 5, 500000, tzinfo=timezone.utc)  # 5.5 seconds later

    result = ddb_util.get_elapsed_time_seconds(start, end)

    assert result == Decimal("5.5")
    assert isinstance(result, Decimal)


@pytest.mark.skip(reason="TODO: Implement test")
def test_calculate_bda_processing_times():
    pass


def test_calculate_wait_time():
    """Test BDA wait time calculation"""
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    with patch("utils.ddb.get_ddb_record") as mock_get_ddb_record:
        mock_get_ddb_record.return_value = {DocumentMetadata.CREATED_AT: created_at.isoformat()}

        with patch("utils.ddb.datetime") as mock_datetime:
            # mock current time to be 10 seconds later
            mock_datetime.now.return_value = datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
            mock_datetime.fromisoformat = datetime.fromisoformat
            wait_time = ddb_util._calculate_wait_time("test-file")
            assert wait_time == Decimal("10.0")


@pytest.mark.skip(reason="TODO: Implement test")
def test_calculate_field_metrics():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_build_completion_timing():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_build_timing_updates():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_build_update_expression():
    pass


def test_execute_ddb_update(mock_ddb_service):
    table_name = "test-table"

    with patch.dict(os.environ, {"DDE_DOCUMENT_METADATA_TABLE_NAME": table_name}):
        object_key = "table-key"
        update_expression = "SET #status = :status"
        expression_values = {":status": "test"}

        ddb_util._execute_ddb_update(object_key, update_expression, expression_values)
        mock_ddb_service.update_item.assert_called_once_with(
            table_name, {"fileName": object_key}, update_expression, expression_values
        )


@pytest.mark.parametrize("user_provided_document_category", ["income", None])
def test_get_user_provided_document_category(user_provided_document_category) -> str:

    with patch("utils.ddb.get_ddb_record") as mock_get_ddb_record:
        mock_get_ddb_record.return_value = {
            DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY: user_provided_document_category
        }

        category = ddb_util.get_user_provided_document_category("test-file")
        assert category == user_provided_document_category


def test_get_ddb_record(mock_ddb_service):
    with patch.dict(os.environ, {"DDE_DOCUMENT_METADATA_TABLE_NAME": "test-table"}):
        mock_ddb_service.get_item.return_value = {
            DocumentMetadata.FILE_NAME: "test-file",
            DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY: "income",
            DocumentMetadata.PROCESS_STATUS: "completed",
        }

        ddb_record = ddb_util.get_ddb_record("test-file")

        for k, v in mock_ddb_service.get_item.return_value.items():
            assert ddb_record[k] == v


@pytest.mark.skip(reason="TODO: Implement test")
def test_get_ddb_by_job_id():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_update_ddb():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_insert_ddb():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_insert_initial_ddb_record():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_set_bda_processing_status_started():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_set_bda_processing_status_not_started():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_classify_as_success():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_classify_as_failed():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_classify_as_not_implemented():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_classify_as_no_document_detected():
    pass


@pytest.mark.skip(reason="TODO: Implement test")
def test_classify_as_no_custom_blueprint_matched():
    pass
