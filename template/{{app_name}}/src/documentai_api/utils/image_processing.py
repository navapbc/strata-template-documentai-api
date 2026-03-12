from documentai_api.config.constants import ConfigDefaults
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def convert_to_grayscale(
    object_key: str, file_bytes: bytes, content_type: str
) -> tuple[bytes, str]:
    """Convert image to grayscale.

    Note: This function converts images to grayscale during upload to reduce memory usage
    and S3 storage costs. Original color files are not preserved (cost optimization).
    """
    if content_type not in ["image/jpeg", "image/png", "image/bmp", "image/tiff"]:
        return file_bytes, content_type

    try:
        import io

        import cv2
        import numpy as np
        from PIL import Image

        nparr = np.frombuffer(file_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return file_bytes, content_type

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # convert to PIL Image for size check and PDF conversion
        pil_image = Image.fromarray(gray)

        # try jpeg first
        jpeg_output = io.BytesIO()
        pil_image.save(jpeg_output, format="JPEG", quality=100)
        jpeg_bytes = jpeg_output.getvalue()

        if len(jpeg_bytes) > int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value):
            logger.info(f"{object_key} too large for BDA, converting to PDF")
            pdf_output = io.BytesIO()
            pil_image.save(pdf_output, format="PDF")
            return pdf_output.getvalue(), "application/pdf"
        else:
            return jpeg_bytes, "image/jpeg"

    except Exception as e:
        logger.error(f"Grayscale conversion failed: {e}")
        return file_bytes, content_type
