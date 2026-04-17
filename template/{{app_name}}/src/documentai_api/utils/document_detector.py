import io
import math
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any, TypeVar

import cv2
import numpy as np
from pdf2image import convert_from_bytes
from PIL import Image

from documentai_api.logging import get_logger
from documentai_api.utils.numbers import normalize

logger = get_logger(__name__)

# increase PIL limit for large document processing
Image.MAX_IMAGE_PIXELS = 250000000
MULTIPAGE_DETECTION_MAX_PAGES = 5
# TODO these should be enums
IMAGE_FILE_TYPES = ["JPEG", "PNG", "GIF", "BMP"]
DOCUMENT_FILE_TYPES = ["PDF", "TIFF"]

T = TypeVar("T")
ImageData = np.ndarray[Any, np.dtype[np.generic]]
ROI = Any


@dataclass
class QualityMetricsRaw:
    """Raw blur detection metrics before normalization."""

    fft_score: float
    edge_score: float
    laplacian_variance: float
    local_contrast: float
    sobel_score: float
    noise_stddev: float
    motion_blur_score: float

    def to_json_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict, replacing NaN with None."""
        metrics_dict = asdict(self)
        for key, value in metrics_dict.items():
            if isinstance(value, float) and math.isnan(value):
                metrics_dict[key] = None
        return metrics_dict


@dataclass
class QualityMetricsNormalized:
    """Normalized blur metrics (0-1 scale)."""

    fft_score: float
    edge_score: float
    laplacian_variance: float
    local_contrast: float
    sobel_score: float
    noise_stddev: float

    def to_json_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict, replacing NaN with None."""
        metrics_dict = asdict(self)
        for key, value in metrics_dict.items():
            if isinstance(value, float) and math.isnan(value):
                metrics_dict[key] = None
        return metrics_dict


@dataclass
class NormalizationRanges:
    """Value ranges for normalizing raw blur metrics to 0-1 scale.

    Ranges are empirically determined from document analysis:
        - fft_score: FFT high-frequency ratio (0.0=blurry, 1.0=sharp)
        - edge_score: Edge pixel density (0.0=no edges, 0.04=text-heavy)
        - laplacian_variance: Laplacian variance (0=blurry, 500+=sharp)
        - local_contrast: Local contrast score (0.0=flat, 1.0=high contrast)
        - sobel_score: Sobel gradient magnitude (0=smooth, 200+=sharp edges)
        - noise_stddev: Noise standard deviation (1=clean, 50+=noisy)
    """

    fft_score: tuple[float, float] = (0.0, 1.0)
    edge_score: tuple[float, float] = (0.0, 0.04)
    laplacian_variance: tuple[float, float] = (0.0, 500.0)
    local_contrast: tuple[float, float] = (0.0, 1.0)
    sobel_score: tuple[float, float] = (0.0, 200.0)
    noise_stddev: tuple[float, float] = (1.0, 50.0)


@dataclass
class DocumentProfile:
    """Document analysis results including quality metrics and content detection."""

    page_count: int | None
    raw_metrics: QualityMetricsRaw | None
    normalized_metrics: QualityMetricsNormalized | None
    normalization_ranges: NormalizationRanges | None
    overall_blur_score: float | None
    is_blurry: bool
    is_multipage: bool
    is_password_protected: bool


class DocumentDetector:
    """Document detection utilities for file type, quality, and structure analysis.

    TODO: Consider replacing multipage_detector.py entirely. The same code exists
    but was kept as to not disturb current processing
    """

    def __init__(self) -> None:
        ##################################
        # configuration parameters
        ##################################
        # cv2 contrast adjustment values
        self.alpha = 2  # Contrast control (e.g., > 1 for increase. Originally 1.5)
        self.beta = 5  # Brightness control (e.g., adds 10 to each pixel)

        # morphology values
        self._iterations = 10

        # cv2 border values
        self.top_border = 25  # 25 pixels on the top
        self.bottom_border = 25  # 25 pixels on the bottom
        self.left_border = 25  # 25 pixels on the left
        self.right_border = 25  # 25 pixels on the right
        self.border_color = [50, 50, 50]  # Black color for BGR images

        # gamma values
        self._gamma_value_darker = 2.2  # example value: > 1.0 to darken the image

        # opencv adaptive thresholding values
        self._maxValue = 255
        self._blockSize = 31  # example: try larger values for documents
        self._C = 2  # example: adjust this value

        # opencv canny values
        self._canny_threshold_1 = 75
        self._canny_threshold_2 = 150
        self._canny_aperture_size = 10

        # opencv GaussianBlur
        self._gaussianblur_ksize = (5, 5)
        self._gaussianblur_sigmaX = 0

    def detect_file_type(self, image_file: bytes) -> str | None:
        """Detect file type from binary header bytes."""
        try:
            file_type = "Unknown"

            if image_file.startswith(b"\xff\xd8"):
                file_type = "JPEG"
            elif image_file.startswith(b"\x89PNG\r\n\x1a\n"):
                file_type = "PNG"
            elif image_file.startswith(b"GIF87a") or image_file.startswith(b"GIF89a"):
                file_type = "GIF"
            elif image_file.startswith(b"%PDF"):
                file_type = "PDF"
            elif image_file.startswith(b"\x49\x49\x2a\x00") or image_file.startswith(
                b"\x4d\x4d\x00\x2a"
            ):
                file_type = "TIFF"
            elif image_file.startswith(b"BM"):
                file_type = "BMP"
            else:
                file_type = "Unknown"

            return file_type

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return None

    def is_pdf(self, image_file: bytes) -> bool:
        """Detect if file is a PDF."""
        return self.detect_file_type(image_file) == "PDF"

    def is_tiff(self, image_file: bytes) -> bool:
        """Detect if file is a TIFF."""
        return self.detect_file_type(image_file) == "TIFF"

    def _get_cv2_laplacian_variance(self, image: ImageData, file_name: str) -> float:
        return float(cv2.Laplacian(image, cv2.CV_64F).var())

    def _split_pdf_into_images(
        self, file_bytes: bytes, max_pages: int | None = None
    ) -> list[ImageData]:
        """Convert PDF pages to grayscale OpenCV images."""
        images: list[ImageData] = []

        if self.is_password_protected(file_bytes):
            logger.warning("DocumentDetector: Password-protected PDF - cannot process")
            return images

        try:
            poppler_path = "/opt/bin" if os.path.exists("/opt/bin") else None
            # pdf_images = convert_from_bytes(file_bytes, poppler_path=poppler_path)

            # only convert up to max_pages if specified
            if max_pages:
                pdf_images = convert_from_bytes(
                    file_bytes, first_page=1, last_page=max_pages, poppler_path=poppler_path
                )
            else:
                pdf_images = convert_from_bytes(file_bytes, poppler_path=poppler_path)

            for page in pdf_images:
                # convert the PIL image to a numpy array
                nparr = np.array(page, dtype=np.uint8)
                img = cv2.cvtColor(nparr, cv2.COLOR_RGB2GRAY)

                del nparr
                images.append(img)

        except Exception as e:
            logger.error(f"Error processing PDF: {e}")
            raise e

        return images

    def _apply_gamma_correction(self, img: ImageData, gamma: float) -> ImageData:
        # Ensure gamma is a float for calculations
        gamma = float(gamma)

        # create a lookup table for gamma correction
        # the values are scaled to the range [0, 1.0] for the pow function,
        # and then back to [0, 255]
        lookup_table = np.array(
            [((i / 255.0) ** gamma) * 255.0 for i in np.arange(256)], dtype="uint8"
        )

        # apply the lookup table to the image
        gamma_corrected_img = cv2.LUT(img, lookup_table)

        return gamma_corrected_img

    def _split_tiff_into_images(
        self, file_bytes: bytes, max_pages: int | None = None
    ) -> list[ImageData]:
        """Convert TIFF frames to grayscale OpenCV images."""
        images = []

        tiff_bytes = io.BytesIO(file_bytes)

        with Image.open(tiff_bytes) as tiff:
            total_frames = tiff.n_frames  # type: ignore[attr-defined]
            frames_to_process = min(total_frames, max_pages) if max_pages else total_frames

            for i in range(frames_to_process):
                tiff.seek(i)
                image = np.array(tiff.copy())
                image = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
                images.append(image)

        return images

    def _truncate_pdf(
        self, file_bytes: bytes, max_pages: int = MULTIPAGE_DETECTION_MAX_PAGES
    ) -> bytes:
        """Extract first N pages from PDF and return as new PDF bytes."""
        import io

        # Convert PDF to images (first N pages only)
        poppler_path = "/opt/bin" if os.path.exists("/opt/bin") else None
        images = convert_from_bytes(
            file_bytes, first_page=1, last_page=max_pages, poppler_path=poppler_path
        )

        if not images:
            return file_bytes

        # Convert images back to PDF
        pdf_bytes = io.BytesIO()
        images[0].save(pdf_bytes, format="PDF", save_all=True, append_images=images[1:])

        return pdf_bytes.getvalue()

    def _truncate_tiff(
        self, file_bytes: bytes, max_pages: int = MULTIPAGE_DETECTION_MAX_PAGES
    ) -> bytes:
        """Extract first N frames from TIFF and return as new TIFF bytes."""
        import io

        from PIL import Image

        tiff_bytes = io.BytesIO(file_bytes)
        output_bytes = io.BytesIO()

        with Image.open(tiff_bytes) as tiff:
            # get total frames
            total_frames = tiff.n_frames  # type: ignore[attr-defined]
            frames_to_process = min(total_frames, max_pages)

            # extract first N frames
            frames = []
            for i in range(frames_to_process):
                tiff.seek(i)
                frame = tiff.copy()
                frames.append(frame)

            # save as new multi-frame TIFF
            if frames:
                frames[0].save(
                    output_bytes,
                    format="TIFF",
                    save_all=True,
                    append_images=frames[1:] if len(frames) > 1 else [],
                )

        return output_bytes.getvalue()

    def _process_image_bytes(
        self,
        file_bytes: bytes,
        file_name: str,
        processor_func: Callable[
            [ImageData, str],
            T,
        ],
    ) -> T:
        """Process image bytes with proper cleanup."""
        nparr = np.frombuffer(file_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)

        if image is None:
            if nparr is not None:
                del nparr
            return None

        result = processor_func(image, file_name)

        del nparr
        del image
        return result

    def _detect_documents_in_image(self, image: ImageData, file_name: str) -> int | None:
        """Detect rectangular document boundaries within a single image.

        Returns:
            int: Number of document-like rectangles found
        """
        self._pages_detected = None
        img = image

        try:
            # repeated closing operation to remove text from the document
            kernel = np.ones((5, 5), np.uint8)
            img = cv2.morphologyEx(img, cv2.MORPH_CLOSE, kernel, iterations=self._iterations)

            # blur the image to improve detection
            img = cv2.GaussianBlur(img, self._gaussianblur_ksize, self._gaussianblur_sigmaX)

            # apply gamma correction
            img = self._apply_gamma_correction(img, self._gamma_value_darker)

            # add a border to the image - this helps with cases where a
            # well-scanned or captured image is white all the way to the edge
            # and has no natural background.
            img = cv2.copyMakeBorder(
                img,
                self.top_border,
                self.bottom_border,
                self.left_border,
                self.right_border,
                cv2.BORDER_CONSTANT,
                value=self.border_color,
            )

            # resize the image for better processing
            height = 500
            h, w = img.shape[:2]
            width = int(w * (height / h))
            img = cv2.resize(img, (width, height))

            # calculate image area
            image_area = img.shape[0] * img.shape[1]

            # perform adaptive thresholding
            img = cv2.adaptiveThreshold(
                img,
                self._maxValue,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY_INV,
                self._blockSize,
                self._C,
            )

            # get the Canny edges from OpenCV
            img = cv2.Canny(
                img, self._canny_threshold_1, self._canny_threshold_2, self._canny_aperture_size
            )

            # find the contours in the edged image, keeping only the
            # largest ones, and initialize the screen contour
            contours = cv2.findContours(img, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

            if len(contours) == 2:
                contours = contours[0]  # OpenCV 4.x
            else:
                contours = contours[1]  # OpenCV 3.x

            # sort the contours by area keeping only the largest 8
            # (since we just need to know whether there is more than 1)
            contours = sorted(contours, key=cv2.contourArea, reverse=True)[:8]

            # set up an array to capture detected pages
            document_contours = []

            # loop the contours and detect whether each is a page or not
            for c in contours:
                # approximate the contour
                peri = cv2.arcLength(c, True)
                approx = cv2.approxPolyDP(c, 0.015 * peri, True)
                contour_area = cv2.contourArea(c)
                contour_area_pct_of_image_area = contour_area / image_area

                # if our approximated contour has four points, then we
                # can assume that we have found our screen
                if (len(approx) == 4) and (contour_area_pct_of_image_area > 0.05):
                    document_contours.append(approx)

            self._pages_detected = len(document_contours)

            return self._pages_detected

        except Exception as e:
            logger.error("An error occurred: ", e)
            return None

    def _extract_document_regions_of_interest(
        self, gray_image: ImageData, file_name: str
    ) -> list[ROI]:
        """Extract content regions from document for quality analysis.

        Returns:
            list: ROI arrays with actual content (non-whitespace)
        """
        grid_size = 3
        h, w = gray_image.shape
        roi_h, roi_w = h // grid_size, w // grid_size

        rois = []
        for i in range(grid_size):
            for j in range(grid_size):
                y_start = i * roi_h
                y_end = (i + 1) * roi_h if i < grid_size - 1 else h
                x_start = j * roi_w
                x_end = (j + 1) * roi_w if j < grid_size - 1 else w

                roi = gray_image[y_start:y_end, x_start:x_end]

                # strict content detection
                edge_pixels = np.sum(cv2.Canny(roi, 50, 150) > 0)  # type: ignore[operator]
                edge_ratio = edge_pixels / roi.size

                # only keep ROIs with some content (not pure whitespace)
                if edge_ratio > 0.01:  # At least 0.1% edges
                    rois.append(roi)

        logger.info(
            f"{file_name}: found {len(rois)} content ROIs out of {grid_size * grid_size} total"
        )
        return rois

    def _calculate_edge_score(self, document_roi: ROI, file_name: str) -> float:
        """Calculate the edge score for a grayscale image.

        Parameters
        ----------
        gray_image : np.ndarray
            Grayscale image (H x W)

        Returns:
        -------
        score : float
            Edge score (0-1), higher = sharper
        """
        # edge density using canny edge detector
        edges_roi = cv2.Canny(document_roi, 100, 200)

        # edge score the ratio of edge pixels to the total number of pixels in the roi
        total_pixels_in_roi = document_roi.size
        # count non-zero pixels
        edge_pixel_count = np.sum(edges_roi > 0)  # type: ignore[operator]

        if total_pixels_in_roi > 0:
            edge_score_raw = edge_pixel_count / total_pixels_in_roi
        else:
            edge_score_raw = np.nan

        return edge_score_raw

    def _calculate_sobel_score(self, document_roi: ROI, file_name: str) -> float:
        gx = cv2.Sobel(document_roi, cv2.CV_64F, 1, 0, ksize=3)
        gy = cv2.Sobel(document_roi, cv2.CV_64F, 0, 1, ksize=3)
        grad_mag = np.sqrt(gx**2 + gy**2)  # type: ignore[operator]
        sobel_score_raw = np.mean(grad_mag)
        return float(sobel_score_raw)

    def _calculate_noise_stddev(self, document_roi: ROI, file_name: str) -> float:
        h, w = document_roi.shape
        noisy_region = document_roi[int(h / 4) : int(3 * h / 4), int(w / 4) : int(3 * w / 4)]
        med = np.median(noisy_region)
        mad = np.median(np.abs(noisy_region - med))
        noise_stddev = mad * 1.4826
        return float(noise_stddev)

    def _get_local_contrast_score(
        self,
        gray_image: ImageData,
        file_name: str,
        patch_size: int = 64,
        ideal_std: float = 60.0,
        mask: ImageData | None = None,
    ) -> float:
        """Compute local contrast score for a grayscale image, optionally ignoring whitespace.

        Parameters
        ----------
        gray_image : np.ndarray
            Grayscale image (H x W)
        patch_size : int
            Size of square patches for computing local contrast
        ideal_std : float
            Reference standard deviation for sharp content
        mask : np.ndarray or None
            Boolean array of same shape as gray. True = content pixel, False = ignore

        Returns:
        -------
        score : float
            Normalized local contrast score (0-1), higher = sharper
        """
        h, w = gray_image.shape
        local_std_list = []

        for y in range(0, h, patch_size):
            for x in range(0, w, patch_size):
                patch = gray_image[y : y + patch_size, x : x + patch_size]

                if mask is not None:
                    patch_mask = mask[y : y + patch_size, x : x + patch_size]
                    if np.sum(patch_mask) == 0:  # no content in this patch
                        continue
                    patch_values = patch[patch_mask]
                else:
                    patch_values = patch

                patch_std = np.std(patch_values)
                local_std_list.append(patch_std)

        if len(local_std_list) == 0:
            return np.nan

        median_std = np.median(local_std_list)
        score = median_std / ideal_std
        return float(score)

    def _calculate_motion_blur_score(self, document_roi: ROI, file_name: str) -> float:
        """Return a motion blur severity score (0 = sharp, 1 = strong motion blur)."""
        kernel_h = np.array([[-1, -1, -1], [2, 2, 2], [-1, -1, -1]], dtype=np.float32)
        kernel_v = np.array([[-1, 2, -1], [-1, 2, -1], [-1, 2, -1]], dtype=np.float32)

        h_response = cv2.filter2D(document_roi, cv2.CV_32F, kernel_h)
        v_response = cv2.filter2D(document_roi, cv2.CV_32F, kernel_v)

        h_var = np.var(h_response)
        v_var = np.var(v_response)

        ratio = max(h_var, v_var) / (min(h_var, v_var) + 1e-6)

        # normalize ratio to 0-1 for easier integration
        score = min((ratio - 1.0) / 9.0, 1.0)  # ratio ~1-10 -> score 0-1
        score = max(0.0, score)
        return float(score)

    def _calculate_quality_metrics(
        self, file_bytes: bytes, file_name: str
    ) -> tuple[QualityMetricsRaw, QualityMetricsNormalized, NormalizationRanges, float] | None:
        """Calculate comprehensive blur metrics for document.

        Returns:
            tuple: (raw_metrics, normalized_metrics, ranges, overall_blur_score)
        """
        try:
            file_type = self.detect_file_type(file_bytes)

            if file_type in IMAGE_FILE_TYPES:
                return self._process_image_bytes(file_bytes, file_name, self._get_quality_metrics)
            elif file_type in DOCUMENT_FILE_TYPES:
                if file_type == "PDF":
                    pages = self._split_pdf_into_images(file_bytes, max_pages=1)
                else:
                    pages = self._split_tiff_into_images(file_bytes, max_pages=1)

                if pages:
                    return self._get_quality_metrics(pages[0], file_name)

            return None

        except Exception as e:
            logger.error(f"Error calculating quality metrics for {file_name}: {e}")
            return None

    def _get_quality_metrics(
        self, gray_image: ImageData, file_name: str
    ) -> tuple[QualityMetricsRaw, QualityMetricsNormalized, NormalizationRanges, float]:
        # get multiple content regions of interest (roi) instead of single roi
        content_rois = self._extract_document_regions_of_interest(gray_image, file_name)

        if not content_rois:
            # fallback to full image if no content found
            content_rois = [gray_image]

        # collect all metrics from all rois
        all_fft = []
        all_edge = []
        all_laplacian = []
        all_contrast = []
        all_sobel = []
        all_noise = []
        all_motion = []

        for _i, roi in enumerate(content_rois):
            # calculate all metrics for a specific region of interest
            fft_score = self.fft_blur_score_normalized(roi, file_name)
            edge_score = self._calculate_edge_score(roi, file_name)
            laplacian_variance = cv2.Laplacian(roi, cv2.CV_64F).var()
            local_contrast = self._get_local_contrast_score(roi, file_name)
            sobel_score = self._calculate_sobel_score(roi, file_name)
            noise_stddev = self._calculate_noise_stddev(roi, file_name)
            motion_blur_score = self._calculate_motion_blur_score(roi, file_name)

            all_fft.append(fft_score)
            all_edge.append(edge_score)
            all_laplacian.append(laplacian_variance)
            all_contrast.append(local_contrast)
            all_sobel.append(sobel_score)
            all_noise.append(noise_stddev)
            all_motion.append(motion_blur_score)

        # create raw metrics from median values
        raw_metrics = QualityMetricsRaw(
            fft_score=float(np.median(all_fft)),
            edge_score=float(np.median(all_edge)),
            laplacian_variance=float(np.median(all_laplacian)),
            local_contrast=float(np.median(all_contrast)),
            sobel_score=float(np.median(all_sobel)),
            noise_stddev=float(np.median(all_noise)),
            motion_blur_score=float(np.median(all_motion)),
        )

        ranges = NormalizationRanges()
        normalized_metrics = QualityMetricsNormalized(
            fft_score=normalize(raw_metrics.fft_score, *ranges.fft_score),
            edge_score=normalize(raw_metrics.edge_score, *ranges.edge_score),
            laplacian_variance=normalize(
                raw_metrics.laplacian_variance, *ranges.laplacian_variance
            ),
            local_contrast=normalize(raw_metrics.local_contrast, *ranges.local_contrast),
            sobel_score=normalize(raw_metrics.sobel_score, *ranges.sobel_score),
            noise_stddev=normalize(raw_metrics.noise_stddev, *ranges.noise_stddev),
        )

        overall_blur_score = 1.0 - (
            0.3 * normalized_metrics.fft_score
            + 0.1 * normalized_metrics.local_contrast
            + 0.6 * normalized_metrics.edge_score
        )

        return raw_metrics, normalized_metrics, ranges, overall_blur_score

    def _is_blurry(
        self,
        raw_metrics: QualityMetricsRaw | None,
        normalized_metrics: QualityMetricsNormalized | None,
        overall_blur_score: float | None,
    ) -> bool:
        """Determine if document is blurry using two-stage detection.

        Returns:
            bool: True if document is considered blurry
        """
        if not raw_metrics or not normalized_metrics or overall_blur_score is None:
            return False

        # high edge + high laplacian = legitimate sharpness (high-res scan)
        if raw_metrics.edge_score > 0.08 and raw_metrics.laplacian_variance > 1000:
            return False  # Definitely sharp

        elif raw_metrics.edge_score > 0.08:  # 0.094 > 0.08
            return True  # Artifacts = blurry

        elif 0.55 <= overall_blur_score < 0.7:
            return bool(raw_metrics.motion_blur_score > 0.25)

        return overall_blur_score >= 0.7

    def _calculate_local_contrast_score(self, file_bytes: bytes, file_name: str) -> float:
        result = self._process_image_bytes(file_bytes, file_name, self._get_local_contrast_score)
        return float(result) if result is not None else np.nan

    def _calculate_laplacian_variance(self, file_bytes: bytes, file_name: str) -> float:
        """Returns the Laplacian variance for an image/document."""
        if not file_bytes:
            logger.warning("No image bytes provided")
            return np.nan

        file_type = self.detect_file_type(file_bytes)

        if file_type in IMAGE_FILE_TYPES:
            result = self._process_image_bytes(
                file_bytes, file_name, self._get_cv2_laplacian_variance
            )
            return float(result) if result is not None else np.nan
        elif file_type in DOCUMENT_FILE_TYPES:
            if file_type == "PDF":
                pages = self._split_pdf_into_images(file_bytes, max_pages=1)
            else:
                pages = self._split_tiff_into_images(file_bytes, max_pages=1)

            return float(self._get_cv2_laplacian_variance(pages[0], file_name)) if pages else np.nan
        else:
            return np.nan

    def fft_blur_score_normalized(self, document_roi: ROI, file: str) -> float:
        if document_roi.size == 0:
            logger.warning("Empty image provided")
            return np.nan

        document_roi_f = np.float32(document_roi)
        f = np.fft.fft2(document_roi_f)
        fshift = np.fft.fftshift(f)
        magnitude = np.abs(fshift)
        h, w = document_roi.shape
        cy, cx = h // 2, w // 2
        radius = min(h, w) // 4
        y, x = np.ogrid[:h, :w]
        dist = np.sqrt((x - cx) ** 2 + (y - cy) ** 2)
        mask_freq = (dist > radius).astype(np.float32)
        high = np.sum(magnitude * mask_freq)
        total = np.sum(magnitude)
        return high / total if total > 0 else np.nan

    def _calculate_frequency_blur_score(self, file_bytes: bytes) -> float:
        """FFT-based blur detection for natural images."""
        file_type = self.detect_file_type(file_bytes)

        if file_type in IMAGE_FILE_TYPES:
            nparr = np.frombuffer(file_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)

            if img is not None:
                f = np.fft.fft2(img)
                fshift = np.fft.fftshift(f)
                magnitude = np.abs(fshift)
                h, w = img.shape
                crow, ccol = h // 2, w // 2
                mask = np.zeros_like(img, dtype=np.uint8)
                cv2.circle(mask, (ccol, crow), 30, 1, -1)
                high_freq_energy = np.sum(magnitude[mask == 0])
                total_energy = np.sum(magnitude)
                score = high_freq_energy / total_energy if total_energy > 0 else np.nan
                del nparr, img
                return score

        return np.nan

    def _get_pdf_page_count(self, file_bytes: bytes) -> int:
        from pypdf import PdfReader

        """Returns actual page count without truncation."""
        try:
            reader = PdfReader(io.BytesIO(file_bytes))
            return len(reader.pages)
        except Exception as e:
            logger.warning(f"Error processing PDF bytes: {e}")
            return 1

    def _get_tiff_page_count(self, file_bytes: bytes) -> int:
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
            logger.warning(f"Error processing TIFF: {e}")
            return 1

    def get_page_count(self, file_bytes: bytes) -> int | None:
        """Count total pages in document."""
        if not file_bytes:
            return None

        file_type = self.detect_file_type(file_bytes)

        if file_type == "PDF":
            return self._get_pdf_page_count(file_bytes)
        elif file_type == "TIFF":
            return self._get_tiff_page_count(file_bytes)
        else:
            return 1  # Single page for JPEG/PNG/etc.

    def is_password_protected(self, file_bytes: bytes) -> bool:
        """Detect if PDF is password protected."""
        file_type = self.detect_file_type(file_bytes)

        if file_type == "PDF":
            return b"/Encrypt" in file_bytes[:4096]

        return False

    def _is_multipage_document(self, file_bytes: bytes, file_name: str) -> bool:
        """Returns True if document contains multiple pages/documents."""
        file_type = self.detect_file_type(file_bytes)
        logger.info(f"DocumentDetector: Processing {file_type} file")

        if file_type in IMAGE_FILE_TYPES:
            documents_in_image = self._process_image_bytes(
                file_bytes, file_name, self._detect_documents_in_image
            )
            return documents_in_image is not None and documents_in_image > 1

        elif file_type in DOCUMENT_FILE_TYPES:
            detected_document_in_page = 0

            if file_type == "PDF":
                pages = self._split_pdf_into_images(
                    file_bytes, max_pages=MULTIPAGE_DETECTION_MAX_PAGES
                )
            else:
                pages = self._split_tiff_into_images(
                    file_bytes, max_pages=MULTIPAGE_DETECTION_MAX_PAGES
                )

            if not pages:
                return False

            for page in pages:
                detected_document_in_page = self._detect_documents_in_image(page, file_name)
                if detected_document_in_page and detected_document_in_page > 1:
                    break

            return detected_document_in_page is not None and detected_document_in_page > 1

        else:
            return False

    def _calculate_edge_metrics(self, file_bytes: bytes) -> tuple[float, float | np.floating[Any]]:
        """Calculate edge density and stddev intensity for any file type."""
        file_type = self.detect_file_type(file_bytes)

        if file_type in IMAGE_FILE_TYPES:
            nparr = np.frombuffer(file_bytes, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
            gray = image
            del nparr

        elif file_type in DOCUMENT_FILE_TYPES:
            if file_type == "PDF":
                pages = self._split_pdf_into_images(file_bytes, max_pages=1)
            else:
                pages = self._split_tiff_into_images(file_bytes, max_pages=1)

            gray = pages[0] if pages else None
        else:
            gray = None

        if gray is not None:
            edges = cv2.Canny(gray, 100, 200)
            edge_density = np.count_nonzero(edges) / edges.size
            stddev_intensity = np.std(gray)
            return edge_density, stddev_intensity

        return np.nan, np.nan

    def get_document_profile(self, file_bytes: bytes, file_name: str) -> DocumentProfile:
        """Analyze document and return comprehensive quality and content metrics.

        Args:
            file_bytes: Raw document/image bytes
            file_name: Filename for debugging

        Returns:
            DocumentProfile: Complete analysis including blur metrics,
            page count, multipage detection, and password protection status

        Note:
            Returns safe defaults (None/False) for invalid input.
        """
        if not file_bytes:
            return DocumentProfile(
                page_count=None,
                raw_metrics=None,
                normalized_metrics=None,
                normalization_ranges=None,
                overall_blur_score=None,
                is_blurry=False,  # no document provided, cannot be blurry
                is_multipage=False,  # no document provided, cannot be multipage
                is_password_protected=False,  # no document provided
            )

        page_count = self.get_page_count(file_bytes)
        is_password_protected = self.is_password_protected(file_bytes)
        quality_metrics = self._calculate_quality_metrics(file_bytes, file_name)

        raw_metrics = quality_metrics[0] if quality_metrics else None
        normalized_metrics = quality_metrics[1] if quality_metrics else None
        normalization_ranges = quality_metrics[2] if quality_metrics else None
        overall_blur_score = quality_metrics[3] if quality_metrics else None

        return DocumentProfile(
            page_count=page_count,
            raw_metrics=raw_metrics,
            normalized_metrics=normalized_metrics,
            normalization_ranges=normalization_ranges,
            overall_blur_score=overall_blur_score,
            is_blurry=bool(
                not is_password_protected
                and self._is_blurry(raw_metrics, normalized_metrics, overall_blur_score)
            ),
            is_multipage=bool(
                not is_password_protected and self._is_multipage_document(file_bytes, file_name)
            ),
            is_password_protected=is_password_protected,
        )

    def truncate_to_pages(
        self, file_bytes: bytes, max_pages: int = MULTIPAGE_DETECTION_MAX_PAGES
    ) -> bytes:
        file_type = self.detect_file_type(file_bytes)

        if file_type == "PDF":
            return self._truncate_pdf(file_bytes, max_pages)
        elif file_type == "TIFF":
            return self._truncate_tiff(file_bytes, max_pages)
        else:
            return file_bytes
