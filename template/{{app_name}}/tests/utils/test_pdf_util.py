# tests/utils/test_pdf.py
from unittest.mock import patch

import pytest

from documentai_api.utils.models import PageMetadata
from documentai_api.utils.pdf import merge_pages_to_pdf


def test_merge_pages_to_pdf_success(mock_s3_service):
    """Test merging PDF and image pages."""
    # Mock S3 returns
    mock_s3_service.get_file_bytes.side_effect = [
        b"%PDF-1.4 fake pdf content",  # page 1 - PDF
        b"\x89PNG fake image content",  # page 2 - image
    ]

    pages = [
        PageMetadata(page_number=1, s3_key="session/page-1.pdf", s3_bucket_name="bucket"),
        PageMetadata(page_number=2, s3_key="session/page-2.png", s3_bucket_name="bucket"),
    ]

    with (
        patch("documentai_api.utils.pdf.PdfWriter"),
        patch("documentai_api.utils.pdf.PdfReader"),
        patch("documentai_api.utils.pdf.Image"),
    ):
        result = merge_pages_to_pdf(pages)

        assert isinstance(result, bytes)
        assert mock_s3_service.get_file_bytes.call_count == 2


def test_merge_pages_to_pdf_unsupported_type(mock_s3_service):
    """Test error on unsupported file type."""
    mock_s3_service.get_file_bytes.return_value = b"fake content"

    pages = [
        PageMetadata(page_number=1, s3_key="session/page-1.txt", s3_bucket_name="bucket"),
    ]

    with pytest.raises(ValueError, match="Unsupported file type: txt"):
        merge_pages_to_pdf(pages)
