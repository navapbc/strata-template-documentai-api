from config.constants import UPLOAD_METADATA_KEYS, ConfigDefaults, ProcessStatus
from schemas.document_metadata import DocumentMetadata
from services import s3 as s3_service
from utils.ddb import (
    classify_as_not_implemented,
    get_ddb_record,
    insert_initial_ddb_record,
    set_bda_processing_status_not_started,
)
from utils.error_handling import handle_lambda_errors
from utils.models import ClassificationData
from utils.s3 import extract_s3_info_from_event, validate_s3_event


def is_file_too_large_for_bda(content_type: str, file_size_bytes: int) -> bool:
    """Check if file exceeds BDA size limits based on content type."""
    if content_type in ["image/jpeg", "image/png"]:
        return file_size_bytes > ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES
    elif content_type in ["application/pdf", "image/tiff"]:
        return file_size_bytes > ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES
    else:
        # unknown file type, assume document limit
        return file_size_bytes > ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES


def convert_to_grayscale(
    object_key: str, file_bytes: bytes, content_type: str
) -> tuple[bytes, str]:
    """Convert image to grayscale, and to PDF if over 5MB."""
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

        if len(jpeg_bytes) > ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES:
            print(f"{object_key} too large for BDA, converting to PDF")
            pdf_output = io.BytesIO()
            pil_image.save(pdf_output, format="PDF")
            return pdf_output.getvalue(), "application/pdf"
        else:
            return jpeg_bytes, "image/jpeg"

    except Exception as e:
        print(f"Grayscale conversion failed: {e}")
        return file_bytes, content_type


def convert_s3_object_to_grayscale(bucket_name: str, object_key: str):
    """Convert S3 image to grayscale in-place."""
    try:
        # download file
        response = s3_service.get_object(bucket_name, object_key)
        file_bytes = response["Body"].read()
        content_type = response.get("ContentType", "application/octet-stream")

        # convert to grayscale
        grayscale_bytes, content_type = convert_to_grayscale(object_key, file_bytes, content_type)

        # upload back (overwrite)
        s3_service.put_object(bucket_name, object_key, grayscale_bytes, content_type)

        print(f"Converted {object_key} to grayscale")
    except Exception as e:
        print(f"Failed to convert {object_key} to grayscale: {e}")


# error handling managed by @handle_lambda_errors decorator
# event validation managed by @validate_s3_event decorator
@handle_lambda_errors
@validate_s3_event
def handler(event, context):
    upload_object_key, upload_bucket_name, metadata = extract_s3_info_from_event(
        event, include_metadata=True
    )

    user_provided_document_category = metadata.get(
        UPLOAD_METADATA_KEYS["user_provided_document_category"]
    )
    job_id = metadata.get(UPLOAD_METADATA_KEYS["job_id"])
    trace_id = metadata.get(UPLOAD_METADATA_KEYS["trace_id"])

    # Check if DDB record exists
    try:
        existing_record = get_ddb_record(upload_object_key)
        status = existing_record.get(DocumentMetadata.PROCESS_STATUS)

        if status == ProcessStatus.PENDING_GRAYSCALE_CONVERSION:
            response = s3_service.head_object(upload_bucket_name, upload_object_key)
            file_size_bytes = response["ContentLength"]
            content_type = response.get("ContentType", "application/octet-stream")

            # second event - process the grayscale file normally
            user_category = existing_record.get(DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY)
            print(f"Processing grayscale file {upload_object_key} with category {user_category}")

            if is_file_too_large_for_bda(content_type, file_size_bytes):
                msg = f"File too large for bda to process {content_type} - {file_size_bytes}"
                classify_as_not_implemented(
                    object_key=upload_object_key,
                    data=ClassificationData(additional_info=msg),
                )

            else:
                set_bda_processing_status_not_started(upload_object_key)

            return {"statusCode": 200}
        else:
            # already processed, skip
            print(f"File {upload_object_key} already processed with status {status}")
            return {"statusCode": 200}

    except ValueError:
        # no ddb record exists - first time processing
        print(f"First time processing {upload_object_key}")

        insert_initial_ddb_record(
            source_bucket_name=upload_bucket_name,
            source_object_key=upload_object_key,
            user_provided_document_category=user_provided_document_category,
            job_id=job_id,
            trace_id=trace_id,
        )

        # check the resulting status - only convert if going to bda
        record = get_ddb_record(upload_object_key)
        status = record.get(DocumentMetadata.PROCESS_STATUS)

        if status == ProcessStatus.PENDING_GRAYSCALE_CONVERSION:
            convert_s3_object_to_grayscale(upload_bucket_name, upload_object_key)
            print(f"Converted {upload_object_key} to grayscale for BDA processing")

        return {"statusCode": 200}
