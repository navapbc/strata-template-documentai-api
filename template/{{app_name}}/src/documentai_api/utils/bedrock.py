import base64
import json
import os
import time

from documentai_api.config.constants import ConfigDefaults
from documentai_api.services.bedrock import invoke_model
from documentai_api.utils.env import (
    BEDROCK_CLASSIFICATION_MODEL_ID_PARAM,
    BEDROCK_CLASSIFICATION_PROMPT_PARAM,
)
from documentai_api.utils.logger import get_logger
from documentai_api.utils.models import BedrockClassificationResult
from documentai_api.utils.ssm import get_parameter_value

logger = get_logger(__name__)


DEFAULT_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"
DEFAULT_PROMPT = (
    "Analyze this image. Respond in JSON only:\n"
    '{"document_type": "string", "confidence": float 0-1, "document_count": int}\n'
    "ONLY use one of these exact values for document_type: <<DOCUMENT_TYPES>>\n"
    "Do not create new categories. If unsure, use 'other_document'.\n"
    "If it's not a document, use 'not_a_document'.\n"
    "document_count: how many separate documents are visible in this image?\n"
)


def _get_model_id() -> str:
    param_name = os.getenv(BEDROCK_CLASSIFICATION_MODEL_ID_PARAM)
    if not param_name:
        return DEFAULT_MODEL_ID
    return get_parameter_value(param_name, default=DEFAULT_MODEL_ID)


def _get_classification_prompt(document_types: list[str]) -> str:
    param_name = os.getenv(BEDROCK_CLASSIFICATION_PROMPT_PARAM)
    if not param_name:
        template = DEFAULT_PROMPT
    else:
        template = get_parameter_value(param_name, default=DEFAULT_PROMPT)
    return template.replace("<<DOCUMENT_TYPES>>", json.dumps(document_types))


def _invoke(messages: list, max_tokens: int = 256) -> dict:
    model_id = _get_model_id()
    logger.info(f"Invoking Bedrock model: {model_id}")
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "messages": messages,
    }
    return invoke_model(model_id=model_id, body=body)


def preclassify_document_image(
    image_bytes: bytes, content_type: str, document_types: list[str]
) -> BedrockClassificationResult:
    """Classify document type and count using Bedrock vision model."""
    if not content_type.startswith("image/"):
        logger.info(f"Non-image content type, skipping classification: {content_type}")
        return BedrockClassificationResult(
            document_type="other_document", confidence=0.0, document_count=1, is_document=True
        )

    if len(image_bytes) > int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value):
        logger.info("Image exceeds 5MB, skipping classification")
        return BedrockClassificationResult(
            document_type="other_document", confidence=0.0, document_count=1, is_document=True
        )

    prompt = _get_classification_prompt(document_types)
    encoded = base64.b64encode(image_bytes).decode("utf-8")
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {"type": "base64", "media_type": content_type, "data": encoded},
                },
                {"type": "text", "text": prompt},
            ],
        }
    ]

    try:
        start = time.time()
        result = _invoke(messages=messages)
        elapsed = round(time.time() - start, 2)

        text = result["content"][0]["text"]
        parsed = json.loads(text)

        document_type = parsed.get("document_type", "other_document")
        valid_types = [*document_types, "other_document", "not_a_document"]
        if document_type not in valid_types:
            document_type = "other_document"

        classification = BedrockClassificationResult(
            document_type=document_type,
            confidence=parsed.get("confidence", 0.0),
            document_count=parsed.get("document_count", 1),
            is_document=parsed.get("is_document", True),
        )

        logger.info(
            f"Pre-classification complete in {elapsed}s: "
            f"type={classification.document_type}, "
            f"confidence={classification.confidence}, "
            f"document_count={classification.document_count}, "
            f"is_document={classification.is_document}"
        )

        return classification
    except Exception as e:
        logger.warning(f"Document classification failed: {e}")
        return BedrockClassificationResult(
            document_type="other_document", confidence=0.0, document_count=1, is_document=True
        )
