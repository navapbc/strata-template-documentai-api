"""Tests for utils/image_processing.py."""

from unittest.mock import MagicMock, patch

from documentai_api.config.constants import ConfigDefaults
from documentai_api.utils.image_processing import convert_to_grayscale


def test_convert_to_grayscale_non_image():
    """Test that non-image files are returned unchanged."""
    file_bytes = b"pdf content"
    result_bytes, result_type = convert_to_grayscale("test.pdf", file_bytes, "application/pdf")

    assert result_bytes == file_bytes
    assert result_type == "application/pdf"


def test_convert_to_grayscale_invalid_image():
    """Test grayscale conversion with invalid image data returns original."""
    with (
        patch("cv2.imdecode") as mock_imdecode,
    ):
        mock_imdecode.return_value = None  # Invalid image

        file_bytes = b"not an image"
        result_bytes, result_type = convert_to_grayscale("test.jpg", file_bytes, "image/jpeg")

        assert result_bytes == file_bytes
        assert result_type == "image/jpeg"


def test_convert_to_grayscale_small_image():
    """Test grayscale conversion with small valid image."""
    with (
        patch("cv2.imdecode") as mock_imdecode,
        patch("cv2.cvtColor") as mock_cvtcolor,
        patch("PIL.Image.fromarray") as mock_fromarray,
    ):
        mock_img = MagicMock()
        mock_imdecode.return_value = mock_img
        mock_cvtcolor.return_value = MagicMock()

        mock_pil = MagicMock()
        mock_fromarray.return_value = mock_pil

        def mock_save(buf, format, quality=None):
            buf.write(b"small jpeg")

        mock_pil.save = mock_save

        result_bytes, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")

        assert result_type == "image/jpeg"
        assert result_bytes == b"small jpeg"
        mock_imdecode.assert_called_once()
        mock_cvtcolor.assert_called_once()


def test_convert_to_grayscale_large_image_converts_to_pdf():
    """Test large grayscale image converts to PDF when > 5MB."""
    with (
        patch("cv2.imdecode") as mock_imdecode,
        patch("cv2.cvtColor") as mock_cvtcolor,
        patch("PIL.Image.fromarray") as mock_fromarray,
    ):
        mock_imdecode.return_value = MagicMock()
        mock_cvtcolor.return_value = MagicMock()

        mock_pil = MagicMock()
        mock_fromarray.return_value = mock_pil

        def save_side_effect(buf, format, quality=None):
            if format == "JPEG":
                # Simulate large JPEG output
                buf.write(b"x" * (int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1))
            else:
                # PDF conversion
                buf.write(b"pdf data")

        mock_pil.save = save_side_effect

        result_bytes, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")

        assert result_type == "application/pdf"
        assert result_bytes == b"pdf data"


def test_convert_to_grayscale_exception_returns_original():
    """Test that exceptions during conversion return original file."""
    with patch("cv2.imdecode") as mock_imdecode:
        mock_imdecode.side_effect = Exception("Conversion error")

        file_bytes = b"image data"
        result_bytes, result_type = convert_to_grayscale("test.jpg", file_bytes, "image/jpeg")

        assert result_bytes == file_bytes
        assert result_type == "image/jpeg"


def test_convert_to_grayscale_png():
    """Test PNG images are processed."""
    with patch("cv2.imdecode") as mock_imdecode:
        mock_imdecode.return_value = None  # Simulate invalid

        file_bytes = b"png data"
        result_bytes, result_type = convert_to_grayscale("test.png", file_bytes, "image/png")

        assert result_bytes == file_bytes
        assert result_type == "image/png"


def test_convert_to_grayscale_bmp():
    """Test BMP images are processed."""
    with patch("cv2.imdecode") as mock_imdecode:
        mock_imdecode.return_value = None

        file_bytes = b"bmp data"
        result_bytes, result_type = convert_to_grayscale("test.bmp", file_bytes, "image/bmp")

        assert result_bytes == file_bytes
        assert result_type == "image/bmp"


def test_convert_to_grayscale_tiff():
    """Test TIFF images are processed."""
    with patch("cv2.imdecode") as mock_imdecode:
        mock_imdecode.return_value = None

        file_bytes = b"tiff data"
        result_bytes, result_type = convert_to_grayscale("test.tiff", file_bytes, "image/tiff")

        assert result_bytes == file_bytes
        assert result_type == "image/tiff"
