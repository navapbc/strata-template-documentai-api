from fastapi.testclient import TestClient

from documentai_api.main import app

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
