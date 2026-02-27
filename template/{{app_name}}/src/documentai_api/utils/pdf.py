"""PDF and document manipulation utilities."""

import io

from PIL import Image
from pypdf import PdfReader, PdfWriter

from documentai_api.services import s3 as s3_service
from documentai_api.utils.logger import get_logger
from documentai_api.utils.models import PageMetadata

logger = get_logger(__name__)


def merge_pages_to_pdf(pages: list[PageMetadata]) -> bytes:
    """Merge multiple pages (PDF/images) from S3 into single PDF.

    Args:
        pages: List of PageMetadata objects with 's3Key', 's3BucketName', and 'pageNumber' fields

    Returns:
        Merged PDF as bytes

    Raises:
        ValueError: If unsupported file type encountered
    """
    writer = PdfWriter()

    # sort pages by page number
    sorted_pages = sorted(pages, key=lambda p: p.page_number)

    for page in sorted_pages:
        s3_key = page.s3_key
        logger.info(f"Processing page {page.page_number}: {s3_key}")

        # download file from S3
        file_bytes = s3_service.get_file_bytes(page.s3_bucket_name, s3_key)

        # determine file type from extension
        file_ext = s3_key.split(".")[-1].lower()

        if file_ext == "pdf":
            _add_pdf_pages(writer, file_bytes)
        elif file_ext in ["jpg", "jpeg", "png", "tiff", "tif"]:
            _add_image_as_pdf_page(writer, file_bytes)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")

    # write merged pdf to bytes
    output = io.BytesIO()
    writer.write(output)
    output.seek(0)
    return output.read()


def _add_pdf_pages(writer: PdfWriter, pdf_bytes: bytes):
    """Add all pages from a PDF to the writer."""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    for page in reader.pages:
        writer.add_page(page)


def _add_image_as_pdf_page(writer: PdfWriter, image_bytes: bytes):
    """Convert image to PDF page and add to writer."""
    image = Image.open(io.BytesIO(image_bytes))

    # convert to rgb if needed (handles png transparency, etc.)
    if image.mode not in ("RGB", "L"):
        image = image.convert("RGB")

    # create pdf from image
    img_pdf_bytes = io.BytesIO()
    image.save(img_pdf_bytes, format="PDF")
    img_pdf_bytes.seek(0)

    # add to merged pdf
    reader = PdfReader(img_pdf_bytes)
    for page in reader.pages:
        writer.add_page(page)
