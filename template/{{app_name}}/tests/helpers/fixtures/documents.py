import pytest


@pytest.fixture
def blank_pdf_bytes():
    from tests.helpers.documents import generate_blank_pdf

    return generate_blank_pdf()


@pytest.fixture
def blank_pdf_file(blank_pdf_bytes, tmp_path):
    file = tmp_path / "test.pdf"
    file.write_bytes(blank_pdf_bytes)
    return file


@pytest.fixture
def empty_zip_bytes():
    import io
    import zipfile

    zip_file = io.BytesIO()
    with zipfile.ZipFile(zip_file, "w") as _f:
        pass

    yield zip_file.getvalue()

    zip_file.close()
