import pytest

from documentai_api.utils.document_detector import (
    DocumentDetector,
    QualityMetricsNormalized,
    QualityMetricsRaw,
)
from tests.helpers.documents import generate_blank_image, generate_blank_pdf, generate_blank_tiff


@pytest.fixture
def detector():
    return DocumentDetector()


@pytest.mark.parametrize(
    ("file_bytes", "expected_type"),
    [
        (b"\xff\xd8", "JPEG"),
        (b"\x89PNG\r\n\x1a\n", "PNG"),
        (b"GIF87a", "GIF"),
        (b"GIF89a", "GIF"),
        (b"%PDF", "PDF"),
        (b"\x49\x49\x2a\x00", "TIFF"),
        (b"\x4d\x4d\x00\x2a", "TIFF"),
        (b"BM", "BMP"),
        (b"UNKNOWN", "Unknown"),
    ],
)
def test_detect_file_type(detector, file_bytes, expected_type):
    assert detector.detect_file_type(file_bytes) == expected_type


@pytest.mark.parametrize(
    ("generator", "expected_type"),
    [
        (lambda: generate_blank_image("JPEG"), "JPEG"),
        (lambda: generate_blank_image("PNG"), "PNG"),
        (lambda: generate_blank_image("GIF"), "GIF"),
        (lambda: generate_blank_image("BMP"), "BMP"),
        (lambda: generate_blank_pdf(), "PDF"),
        (lambda: generate_blank_tiff(), "TIFF"),
    ],
)
def test_detect_file_type_generated_files(detector, generator, expected_type):
    """Test file type detection with all generated file types."""
    assert detector.detect_file_type(generator()) == expected_type


def test_is_pdf(detector):
    assert detector.is_pdf(b"%PDF-1.4") is True
    assert detector.is_pdf(b"\xff\xd8") is False


def test_is_tiff(detector):
    assert detector.is_tiff(b"\x49\x49\x2a\x00") is True
    assert detector.is_tiff(b"%PDF") is False


def test_is_password_protected_true(detector):
    pdf_with_encrypt = b"%PDF-1.4\n" + b"/Encrypt" + b"\x00" * 4000
    assert detector.is_password_protected(pdf_with_encrypt) is True


def test_is_password_protected_false(detector):
    pdf_without_encrypt = b"%PDF-1.4\n" + b"\x00" * 4000
    assert detector.is_password_protected(pdf_without_encrypt) is False


def test_is_password_protected_non_pdf(detector):
    assert detector.is_password_protected(b"\xff\xd8") is False


@pytest.mark.parametrize(
    ("generator", "expected_page_count"),
    [
        (lambda: generate_blank_image("JPEG"), 1),
        (lambda: generate_blank_image("PNG"), 1),
        (lambda: generate_blank_image("GIF"), 1),
        (lambda: generate_blank_image("BMP"), 1),
        (lambda: generate_blank_pdf(), 1),
        (lambda: generate_blank_tiff(), 1),
        (lambda: generate_blank_pdf(num_pages=3), 3),
        (lambda: generate_blank_tiff(num_pages=3), 3),
        (lambda: generate_blank_pdf(num_pages=20), 20),
        (lambda: generate_blank_tiff(num_pages=20), 20),
    ],
)
def test_get_page_count(detector, generator, expected_page_count):
    assert detector.get_page_count(generator()) == expected_page_count


def test_quality_metrics_raw_to_json_dict_with_nan():
    metrics = QualityMetricsRaw(
        fft_score=0.5,
        edge_score=float("nan"),
        laplacian_variance=100.0,
        local_contrast=float("nan"),
        sobel_score=50.0,
        noise_stddev=10.0,
        motion_blur_score=0.2,
    )
    result = metrics.to_json_dict()
    assert result["fft_score"] == 0.5
    assert result["edge_score"] is None
    assert result["laplacian_variance"] == 100.0
    assert result["local_contrast"] is None


def test_quality_metrics_normalized_to_json_dict_with_nan():
    metrics = QualityMetricsNormalized(
        fft_score=0.5,
        edge_score=float("nan"),
        laplacian_variance=0.8,
        local_contrast=float("nan"),
        sobel_score=0.6,
        noise_stddev=0.3,
    )
    result = metrics.to_json_dict()
    assert result["fft_score"] == 0.5
    assert result["edge_score"] is None
    assert result["local_contrast"] is None


def test_get_document_profile_empty(detector):
    """Test with empty input."""
    profile = detector.get_document_profile(b"", "empty.pdf")
    assert profile.page_count is None
    assert profile.is_blurry is False
    assert profile.is_multipage is False
    assert profile.is_password_protected is False


@pytest.mark.parametrize(
    ("generator", "is_image", "max_pages", "expected_count"),
    [
        (lambda: generate_blank_image("JPEG"), True, 1, 1),
        (lambda: generate_blank_image("PNG"), True, 1, 1),
        (lambda: generate_blank_image("GIF"), True, 1, 1),
        (lambda: generate_blank_image("BMP"), True, 1, 1),
        (lambda: generate_blank_pdf(), False, 3, 1),
        (lambda: generate_blank_tiff(), False, 3, 1),
        (lambda: generate_blank_pdf(num_pages=20), False, 3, 3),
        (lambda: generate_blank_pdf(num_pages=20), False, 6, 6),
        (lambda: generate_blank_tiff(num_pages=20), False, 3, 3),
        (lambda: generate_blank_tiff(num_pages=20), False, 6, 6),
    ],
)
def test_truncate_to_pages(detector, generator, is_image, max_pages, expected_count):
    bytes = generator()
    truncated = detector.truncate_to_pages(bytes, max_pages=max_pages)

    if not is_image:
        assert detector.get_page_count(truncated) == expected_count
    else:
        assert truncated == bytes  # should return unchanged


def test_get_document_profile_with_document(detector):
    """Test get_document_profile with actual document bytes."""
    pdf_bytes = generate_blank_pdf()
    profile = detector.get_document_profile(pdf_bytes, "test.pdf")

    # basic properties should be populated
    assert profile.page_count == 1
    assert profile.is_password_protected is False

    # quality metrics should be calculated (not None)
    assert profile.raw_metrics is not None
    assert profile.normalized_metrics is not None
    assert profile.normalization_ranges is not None
    assert profile.overall_blur_score is not None

    # blur detection should work (blank white page is not blurry)
    assert isinstance(profile.is_blurry, bool)
    assert isinstance(profile.is_multipage, bool)


def test_get_document_profile_password_protected(detector):
    """Test get_document_profile with password-protected PDF."""
    # create a fake password-protected PDF (has /Encrypt in header)
    pdf_with_encrypt = b"%PDF-1.4\n" + b"/Encrypt" + b"\x00" * 4000
    profile = detector.get_document_profile(pdf_with_encrypt, "protected.pdf")

    # should detect as password protected
    assert profile.is_password_protected is True

    # should still get page count (returns 1 on error)
    assert profile.page_count == 1

    # quality metrics should be None (can't analyze encrypted PDF)
    assert profile.raw_metrics is None
    assert profile.normalized_metrics is None

    # should not be marked as blurry or multipage (can't analyze)
    assert profile.is_blurry is False
    assert profile.is_multipage is False
