from unittest.mock import patch

from fastapi.testclient import TestClient

from documentai_api.app import app

client = TestClient(app)

MOCK_SCHEMAS = {
    "W2": {
        "fields": [
            {"name": "ssn", "type": "string", "description": "Social security number"},
            {"name": "wages", "type": "number", "description": "Total wages"},
        ]
    },
    "Payslip": {
        "fields": [
            {"name": "gross_pay", "type": "number", "description": "Gross pay amount"},
            {"name": "ssn", "type": "string", "description": "Employee SSN"},
        ]
    },
}


def _mock_all_schemas():
    return patch("documentai_api.app.get_all_schemas", return_value=MOCK_SCHEMAS)


# ==============================================================================
# schema list
# ==============================================================================


def test_schemas_list():
    """Test listing all schemas."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/schemas")

    assert response.status_code == 200
    assert response.json()["schemas"] == ["Payslip", "W2"]


# ==============================================================================
# single schema
# ==============================================================================


def test_schema_single():
    """Test getting single schema."""
    with patch("documentai_api.app.get_document_schema") as mock:
        mock.return_value = MOCK_SCHEMAS["W2"]

        response = client.get("/v1/dictionary/schemas/W2")

    assert response.status_code == 200
    assert len(response.json()["fields"]) == 2


def test_schema_not_found():
    """Test 404 for unknown schema."""
    with patch("documentai_api.app.get_document_schema", return_value=None):
        response = client.get("/v1/dictionary/schemas/Unknown")

    assert response.status_code == 404


# ==============================================================================
# all schemas
# ==============================================================================


def test_all_json():
    """Test getting all fields as JSON."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/schemas/all")

    fields = response.json()["fields"]
    assert len(fields) == 4
    assert fields[0]["documentType"] == "Payslip"
    assert fields[-1]["documentType"] == "W2"


def test_all_csv():
    """Test getting all fields as CSV."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/schemas/all?format=csv")

    assert response.status_code == 200
    assert "text/csv" in response.headers["content-type"]
    assert "W2" in response.text
    assert "Payslip" in response.text


# ==============================================================================
# search
# ==============================================================================


def test_search_no_query():
    """Search with no query returns all fields."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search")

    assert response.status_code == 200
    assert len(response.json()["fields"]) == 4


def test_search_by_name():
    """Search filtered to name field."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=ssn&field=name")

    fields = response.json()["fields"]
    assert len(fields) == 2
    assert all(f["name"] == "ssn" for f in fields)


def test_search_by_description():
    """Search filtered to description field."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=social&field=description")

    fields = response.json()["fields"]
    assert len(fields) == 1
    assert fields[0]["documentType"] == "W2"


def test_search_by_document_type():
    """Search filtered to documentType field."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=payslip&field=documentType")

    fields = response.json()["fields"]
    assert len(fields) == 2
    assert all(f["documentType"] == "Payslip" for f in fields)


def test_search_all_columns():
    """Search with no field searches all columns."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=gross")

    fields = response.json()["fields"]
    assert len(fields) == 1
    assert fields[0]["name"] == "gross_pay"


def test_search_case_insensitive():
    """Search is case-insensitive."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=SSN&field=name")

    assert len(response.json()["fields"]) == 2


def test_search_no_results():
    """Search with no matches returns empty list."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=nonexistent")

    assert len(response.json()["fields"]) == 0


def test_search_csv():
    """Search with CSV format."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=ssn&field=name&format=csv")

    assert "text/csv" in response.headers["content-type"]
    assert "W2" in response.text
    assert "Payslip" in response.text


def test_search_sorted():
    """Results are sorted by documentType."""
    with _mock_all_schemas():
        response = client.get("/v1/dictionary/search?q=ssn&field=name")

    fields = response.json()["fields"]
    assert fields[0]["documentType"] == "Payslip"
    assert fields[1]["documentType"] == "W2"
