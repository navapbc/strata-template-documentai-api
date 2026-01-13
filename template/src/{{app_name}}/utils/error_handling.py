import logging
import traceback
from functools import wraps
from typing import Any, Callable

# configure logging for lambda
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def handle_lambda_errors(handler_func: Callable) -> Callable:
    """Decorator to standardize Lambda error handling and logging."""

    @wraps(handler_func)
    def wrapper(event: dict[str, Any], context: Any) -> dict[str, Any]:
        try:
            return handler_func(event, context)
        except Exception as e:
            error_msg = f"Handler {handler_func.__name__} failed: {e}"
            stack_trace = traceback.format_exc()

            print(f"ERROR: {error_msg}")
            print(f"STACK TRACE:\n{stack_trace}")

            logging.error(error_msg)
            logging.error(stack_trace)

            # try to update DDB status to failed
            try:
                from config.settings import (
                    UPLOAD_METADATA_KEYS,
                    DocumentCategory
                )
                from utils.ddb import ClassificationData, classify_as_failed
                from utils.s3 import extract_s3_info_from_event

                object_key, _, metadata = extract_s3_info_from_event(event, include_metadata=True)

                user_provided_document_category = metadata[
                    UPLOAD_METADATA_KEYS["user_provided_document_category"]
                ]

                # for BDA output processor, extract the actual uploaded filename
                if "processed/" in object_key:
                    # object_key: processed/w2-abc123.pdf/job-id/0/custom_output/0/result.json
                    # extract w2-abc123.pdf
                    path_parts = object_key.split("/")
                    if len(path_parts) >= 2:
                        filename = path_parts[1]  # get the uploaded filename
                    else:
                        filename = object_key  # fallback
                else:
                    filename = object_key

                classify_as_failed(
                    object_key=filename,
                    error_message=error_msg,
                    data=ClassificationData(additional_info=str(e)),
                )
                print(f"Updated DDB status to failed for {filename}")
            except Exception as ddb_error:
                print(f"Failed to update DDB status: {ddb_error}")

            return {"statusCode": 500, "body": str(e)}

    return wrapper


__all__ = ["handle_lambda_errors"]
