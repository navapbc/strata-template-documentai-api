"""Lightweight document utilities. No CV dependencies."""

import io

from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def _get_pdf_page_count(file_bytes: bytes) -> int:
    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        return len(reader.pages)
    except Exception as e:
        logger.warning(f"Error getting PDF page count: {e}")
        return 1


def _get_tiff_page_count(file_bytes: bytes) -> int:
    from PIL import Image

    try:
        with Image.open(io.BytesIO(file_bytes)) as img:
            page_count = 0
            while True:
                try:
                    img.seek(page_count)
                    page_count += 1
                except EOFError:
                    break
            return page_count
    except Exception as e:
        logger.warning(f"Error getting TIFF page count: {e}")
        return 1


def detect_file_type(file_bytes: bytes) -> str:
    """Detect file type from binary header bytes."""
    if not file_bytes:
        return "Unknown"

    if file_bytes.startswith(b"\xff\xd8"):
        return "JPEG"
    elif file_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    elif file_bytes.startswith(b"GIF87a") or file_bytes.startswith(b"GIF89a"):
        return "GIF"
    elif file_bytes.startswith(b"%PDF"):
        return "PDF"
    elif file_bytes.startswith(b"\x49\x49\x2a\x00") or file_bytes.startswith(b"\x4d\x4d\x00\x2a"):
        return "TIFF"
    elif file_bytes.startswith(b"BM"):
        return "BMP"
    return "Unknown"


def is_password_protected(file_bytes: bytes) -> bool:
    """Detect if PDF is password protected."""
    if detect_file_type(file_bytes) == "PDF":
        return b"/Encrypt" in file_bytes[:4096]
    return False


def get_page_count(file_bytes: bytes) -> int | None:
    """Count total pages in document."""
    if not file_bytes:
        return None

    file_type = detect_file_type(file_bytes)

    if file_type == "PDF":
        return _get_pdf_page_count(file_bytes)
    elif file_type == "TIFF":
        return _get_tiff_page_count(file_bytes)
    return 1
