import traceback
from functools import wraps
from typing import Any, Callable

from utils.logger import get_logger

logger = get_logger(__name__)


def handle_lambda_errors(handler_func: Callable) -> Callable:
    """Decorator to standardize Lambda error handling and logging."""

    @wraps(handler_func)
    def wrapper(event: dict[str, Any], context: Any) -> dict[str, Any]:
        try:
            return handler_func(event, context)
        except Exception as e:
            error_msg = f"Handler {handler_func.__name__} failed: {e}"
            stack_trace = traceback.format_exc()

            logger.error(f"ERROR: {error_msg}")
            logger.error(f"STACK TRACE:\n{stack_trace}")

            # try to update DDB status to failed
            try:
                from config.constants import BDA_PROCESSED_FILE_PREFIX
                from utils.ddb import ClassificationData, classify_as_failed
                from utils.s3 import extract_s3_info_from_event

                object_key, _ = extract_s3_info_from_event(event)

                # for BDA output processor, extract the actual uploaded filename
                if object_key.startswith(f"{BDA_PROCESSED_FILE_PREFIX}/"):
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
                logger.warning(f"Updated DDB status to failed for {filename}")
            except Exception as ddb_error:
                logger.error(f"Failed to update DDB status: {ddb_error}")

            return {"statusCode": 500, "body": str(e)}

    return wrapper


__all__ = ["handle_lambda_errors"]
