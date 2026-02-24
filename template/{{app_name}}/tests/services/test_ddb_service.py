"""Tests for services/ddb.py."""

from documentai_api.services import ddb as ddb_service


def test_get_item(ddb_table):
    """Get item from DynamoDB table."""
    ddb_table.put_item(Item={"id": "123", "name": "test"})
    result = ddb_service.get_item(ddb_table.name, {"id": "123"})
    assert result == {"id": "123", "name": "test"}


def test_get_item_not_found(ddb_table):
    """Get item returns None when not found."""
    result = ddb_service.get_item(ddb_table.name, {"id": "123"})
    assert result is None


def test_get_item_eventual_consistency(ddb_table):
    """Get item with eventual consistency."""
    ddb_table.put_item(Item={"id": "123"})
    result = ddb_service.get_item(ddb_table.name, {"id": "123"}, consistent_read=False)
    assert result == {"id": "123"}


def test_put_item(ddb_table):
    """Put item to DynamoDB table."""
    item = {"id": "123", "name": "test"}
    ddb_service.put_item(ddb_table.name, item)

    response = ddb_table.get_item(Key={"id": "123"})
    assert response["Item"] == item


def test_update_item(ddb_table):
    """Update item in DynamoDB table."""
    ddb_table.put_item(Item={"id": "123", "description": "old"})

    key = {"id": "123"}
    update_expr = "SET description = :description"
    expr_values = {":description": "updated"}

    ddb_service.update_item(ddb_table.name, key, update_expr, expr_values)

    response = ddb_table.get_item(Key={"id": "123"})
    assert response["Item"]["description"] == "updated"


def test_query_by_key(ddb_table):
    """Query DynamoDB table by key using GSI."""
    ddb_table.put_item(Item={"id": "123", "userId": "user-123"})
    ddb_table.put_item(Item={"id": "456", "userId": "user-123"})

    result = ddb_service.query_by_key(
        ddb_table.name, ddb_table.test_index_name, "userId", "user-123"
    )

    assert len(result) == 2
    assert {item["id"] for item in result} == {"123", "456"}


def test_query_by_key_no_results(ddb_table):
    """Query returns empty list when no items found."""
    result = ddb_service.query_by_key(
        ddb_table.name, ddb_table.test_index_name, "userId", "user-999"
    )
    assert result == []
