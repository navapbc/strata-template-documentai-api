"""Tests for utils/schemas.py."""

from unittest.mock import MagicMock, patch

import pytest

from documentai_api.utils import env, schemas


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Mock environment variables."""
    monkeypatch.setenv(env.BDA_PROJECT_ARN, "arn:aws:bedrock:us-east-1:123:project/test")


@pytest.fixture
def mock_cache():
    """Mock cache."""
    with patch("documentai_api.utils.schemas.get_cache") as mock:
        cache = MagicMock()
        mock.return_value = cache
        yield cache


@pytest.fixture
def mock_bda_services():
    """Mock BDA service calls."""
    with (
        patch("documentai_api.utils.schemas.get_data_automation_project") as mock_bda_project,
        patch("documentai_api.utils.schemas.get_blueprint") as mock_bda_blueprint,
    ):
        yield {"project": mock_bda_project, "blueprint": mock_bda_blueprint}


@pytest.fixture
def mock_bda_project(mock_bda_services):
    return mock_bda_services["project"]


@pytest.fixture
def mock_bda_blueprint(mock_bda_services):
    return mock_bda_services["blueprint"]


def test_extract_fields():
    """Extract basic fields from schema."""
    schema = {
        "properties": {
            "name": {"type": "string", "instruction": "Customer name"},
            "age": {"type": "number", "instruction": "Customer age"},
        }
    }

    fields = schemas._extract_fields(schema)

    assert len(fields) == 2
    assert fields[0]["name"] == "name"
    assert fields[0]["type"] == "string"
    assert fields[1]["name"] == "age"


def test_extract_fields_with_ref():
    """Extract nested fields with $ref."""
    schema = {
        "properties": {"address": {"$ref": "#/definitions/Address"}},
        "definitions": {
            "Address": {
                "properties": {
                    "street": {"type": "string", "instruction": "Street name"},
                    "city": {"type": "string", "instruction": "City name"},
                }
            }
        },
    }

    fields = schemas._extract_fields(schema)

    assert len(fields) == 2
    assert fields[0]["name"] == "address.street"
    assert fields[1]["name"] == "address.city"


def test_extract_fields_array_with_ref():
    """Extract array fields with $ref."""
    schema = {
        "properties": {"items": {"type": "array", "items": {"$ref": "#/definitions/Item"}}},
        "definitions": {
            "Item": {
                "properties": {
                    "name": {"type": "string", "instruction": "Item name"},
                    "price": {"type": "number", "instruction": "Item price"},
                }
            }
        },
    }

    fields = schemas._extract_fields(schema)

    assert len(fields) == 2
    assert fields[0]["name"] == "items.name"
    assert fields[1]["name"] == "items.price"


def test_extract_fields_array_without_ref():
    """Extract simple array fields."""
    schema = {"properties": {"tags": {"type": "array", "instruction": "List of tags"}}}

    fields = schemas._extract_fields(schema)

    assert len(fields) == 1
    assert fields[0]["name"] == "tags"
    assert fields[0]["type"] == "array"


def test_get_all_schemas_from_cache(mock_cache):
    """Get schemas from cache when available."""
    cached_schemas = {"Invoice": {"documentType": "Invoice", "fields": []}}
    mock_cache.get.return_value = cached_schemas

    result = schemas.get_all_schemas()

    assert result == cached_schemas
    mock_cache.get.assert_called_once_with("blueprint_schemas")
    mock_cache.add.assert_not_called()


def test_get_all_schemas_fetch_and_cache(mock_cache, mock_bda_services):
    """Fetch schemas from BDA and cache them."""
    mock_cache.get.return_value = None

    mock_bda_services["project"].return_value = {
        "project": {
            "customOutputConfiguration": {
                "blueprints": [{"blueprintArn": "arn:aws:bedrock:us-east-1:123:blueprint/invoice"}]
            }
        }
    }

    mock_bda_services["blueprint"].return_value = {
        "blueprint": {"schema": '{"class": "Invoice", "properties": {}}'}
    }

    result = schemas.get_all_schemas()

    assert "Invoice" in result
    mock_cache.add.assert_called_once()


def test_get_document_schema_found(mock_cache):
    """Get specific document schema."""
    mock_cache.get.return_value = {
        "Invoice": {"documentType": "Invoice", "fields": []},
        "Receipt": {"documentType": "Receipt", "fields": []},
    }

    result = schemas.get_document_schema("Invoice")

    assert result["documentType"] == "Invoice"


def test_get_document_schema_not_found(mock_cache):
    """Return None when document type not found."""
    mock_cache.get.return_value = {"Invoice": {"documentType": "Invoice", "fields": []}}

    result = schemas.get_document_schema("Unknown")

    assert result is None


def test_invalidate_schema_cache(mock_cache):
    """Invalidate schema cache."""
    schemas.invalidate_schema_cache()

    mock_cache.invalidate.assert_called_once_with("blueprint_schemas")
