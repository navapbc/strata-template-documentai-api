"""Tests for utils/zip.py."""

from io import BytesIO
from unittest.mock import AsyncMock, MagicMock
from zipfile import ZipFile

import pytest

from documentai_api.utils.zip import extract_files_from_zip


@pytest.mark.asyncio
async def test_extract_files_from_zip_success():
    """Test extracting files from ZIP."""
    # create test ZIP
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as zf:
        zf.writestr("doc1.pdf", b"pdf content 1")
        zf.writestr("doc2.pdf", b"pdf content 2")
    zip_buffer.seek(0)

    # create mock UploadFile
    mock_zip = MagicMock()
    mock_zip.read = AsyncMock(return_value=zip_buffer.getvalue())

    files = await extract_files_from_zip(mock_zip)

    assert len(files) == 2
    assert files[0].filename == "doc1.pdf"
    assert files[1].filename == "doc2.pdf"
    assert files[0].file.read() == b"pdf content 1"
    files[0].file.seek(0)
    assert files[1].file.read() == b"pdf content 2"


@pytest.mark.asyncio
async def test_extract_files_from_zip_nested_directories():
    """Test extracting files from nested directories in ZIP."""
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as zf:
        zf.writestr("folder1/doc1.pdf", b"pdf 1")
        zf.writestr("folder1/subfolder/doc2.pdf", b"pdf 2")
        zf.writestr("doc3.pdf", b"pdf 3")
    zip_buffer.seek(0)

    mock_zip = MagicMock()
    mock_zip.read = AsyncMock(return_value=zip_buffer.getvalue())

    files = await extract_files_from_zip(mock_zip)

    assert len(files) == 3
    # should use basename only
    assert files[0].filename == "doc1.pdf"
    assert files[1].filename == "doc2.pdf"
    assert files[2].filename == "doc3.pdf"


@pytest.mark.asyncio
async def test_extract_files_from_zip_skips_directories():
    """Test that directory entries are skipped."""
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w") as zf:
        zf.writestr("folder1/", "")  # directory entry
        zf.writestr("folder1/doc1.pdf", b"pdf 1")
    zip_buffer.seek(0)

    mock_zip = MagicMock()
    mock_zip.read = AsyncMock(return_value=zip_buffer.getvalue())

    files = await extract_files_from_zip(mock_zip)

    assert len(files) == 1
    assert files[0].filename == "doc1.pdf"


@pytest.mark.asyncio
async def test_extract_files_from_zip_empty():
    """Test extracting from empty ZIP."""
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w"):
        pass  # empty zip
    zip_buffer.seek(0)

    mock_zip = MagicMock()
    mock_zip.read = AsyncMock(return_value=zip_buffer.getvalue())

    files = await extract_files_from_zip(mock_zip)

    assert len(files) == 0
