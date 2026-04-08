from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def analyze_id(image_bytes: bytes) -> dict:
    """Call Textract AnalyzeID for identity documents."""
    logger.info(f"Calling Textract AnalyzeID ({len(image_bytes)} bytes)")
    try:
        client = AWSClientFactory.get_textract_client()
        response = client.analyze_id(
            DocumentPages=[{"Bytes": image_bytes}]
        )
        doc_count = len(response.get("IdentityDocuments", []))
        logger.info(f"Textract AnalyzeID returned {doc_count} identity document(s)")
        return response
    except Exception as e:
        logger.error(f"Textract AnalyzeID failed: {e}")
        raise
