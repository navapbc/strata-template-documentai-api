"""Tests for services/ddb.py"""

from unittest.mock import MagicMock, patch

import pytest
from services import ddb as ddb_service


@pytest.fixture(autouse=True)
def mock_ddb_table():
    with patch("services.ddb.AWSClientFactory.get_ddb_table") as mock_get_table:
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table
        yield mock_table


def test_get_item(mock_ddb_table):
    """Get item from DynamoDB table"""
    mock_ddb_table.get_item.return_value = {"Item": {"id": "123", "name": "test"}}
    result = ddb_service.get_item("test-table", {"id": "123"})
    mock_ddb_table.get_item.assert_called_once_with(Key={"id": "123"}, ConsistentRead=True)
    assert result == {"id": "123", "name": "test"}


def test_get_item_not_found(mock_ddb_table):
    """Get item returns None when not found"""
    mock_ddb_table.get_item.return_value = {}
    result = ddb_service.get_item("test-table", {"id": "123"})
    assert result is None


def test_get_item_eventual_consistency(mock_ddb_table):
    """Get item with eventual consistency"""
    mock_ddb_table.get_item.return_value = {"Item": {"id": "123"}}
    ddb_service.get_item("test-table", {"id": "123"}, consistent_read=False)
    mock_ddb_table.get_item.assert_called_once_with(Key={"id": "123"}, ConsistentRead=False)


def test_put_item(mock_ddb_table):
    """Put item to DynamoDB table"""
    item = {"id": "123", "name": "test"}
    ddb_service.put_item("test-table", item)
    mock_ddb_table.put_item.assert_called_once_with(Item=item)


def test_update_item(mock_ddb_table):
    """Update item in DynamoDB table"""
    key = {"id": "123"}
    update_expr = "SET #name = :name"
    expr_values = {":name": "updated"}

    ddb_service.update_item("test-table", key, update_expr, expr_values)

    mock_ddb_table.update_item.assert_called_once_with(
        Key=key, UpdateExpression=update_expr, ExpressionAttributeValues=expr_values
    )


def test_query_by_key(mock_ddb_table):
    """Query DynamoDB table by key using GSI"""
    mock_ddb_table.query.return_value = {"Items": [{"id": "123"}, {"id": "456"}]}

    with patch("boto3.dynamodb.conditions.Key") as mock_key_class:
        mock_key = MagicMock()
        mock_key_class.return_value = mock_key
        mock_key.eq.return_value = "mocked"

        result = ddb_service.query_by_key("test-table", "test-index", "userId", "user-123")

        assert len(result) == 2
        assert result[0]["id"] == "123"
        mock_ddb_table.query.assert_called_once()


def test_query_by_key_no_results(mock_ddb_table):
    """Query returns empty list when no items found"""
    mock_ddb_table.query.return_value = {}

    with patch("boto3.dynamodb.conditions.Key") as mock_key_class:
        mock_key = MagicMock()
        mock_key_class.return_value = mock_key
        mock_key.eq.return_value = "mocked"

        result = ddb_service.query_by_key("test-table", "test-index", "userId", "user-123")

        assert result == []
