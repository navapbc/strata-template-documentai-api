"""Tests for jobs/document_processor/main.py."""

from unittest.mock import MagicMock

import pytest

from documentai_api.config.constants import ConfigDefaults, ProcessStatus
from documentai_api.jobs.document_processor.main import (
    convert_s3_object_to_grayscale,
    convert_to_grayscale,
    invoke_bda,
    is_file_too_large_for_bda,
    main,
)
from documentai_api.schemas.document_metadata import DocumentMetadata


@pytest.fixture(autouse=True)
def disable_tenacity_wait_auto(disable_tenacity_wait):
    pass


@pytest.fixture(autouse=True)
def mock_env(runtime_required_env):
    pass


@pytest.fixture(autouse=True)
def mock_invoke(mocker):
    return mocker.patch("documentai_api.jobs.document_processor.main.invoke_bda")


@pytest.fixture
def input_image(s3_bucket):
    return s3_bucket.put_object(
        Key="input/test.jpg",
        Body=b"image data",
        ContentType="image/jpeg",
        Metadata={
            "job-id": "test-job-id",
            "trace-id": "test-trace-id",
            "user-provided-document-category": "income",
            "original-file-name": "original.jpg",
        },
    )


@pytest.fixture
def input_pdf(s3_bucket):
    return s3_bucket.put_object(
        Key="input/test.pdf",
        Body=b"PDF data",
        ContentType="application/pdf",
        Metadata={
            "job-id": "test-job-id",
            "trace-id": "test-trace-id",
            "user-provided-document-category": "income",
            "original-file-name": "original.pdf",
        },
    )


@pytest.mark.parametrize(
    ("content_type", "file_size", "expected"),
    [
        ("image/jpeg", ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value, False),
        ("image/jpeg", int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1, True),
        ("image/png", ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value, False),
        ("image/png", int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1, True),
        ("application/pdf", ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value, False),
        ("application/pdf", int(ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value) + 1, True),
        ("image/tiff", ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value, False),
        ("image/tiff", int(ConfigDefaults.BDA_MAX_DOCUMENT_FILE_SIZE_BYTES.value) + 1, True),
        ("unknown/type", int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1, True),
    ],
)
def test_is_file_too_large_for_bda(content_type, file_size, expected):
    """Test file size validation for BDA limits."""
    result = is_file_too_large_for_bda(content_type, file_size)
    assert result == expected


def test_convert_to_grayscale_non_image():
    """Test that non-image files are returned unchanged."""
    file_bytes = b"pdf content"
    result_bytes, result_type = convert_to_grayscale("test.pdf", file_bytes, "application/pdf")

    assert result_bytes == file_bytes
    assert result_type == "application/pdf"


def test_convert_to_grayscale_invalid_image():
    """Test grayscale conversion with invalid image data."""
    file_bytes = b"not an image"
    result_bytes, result_type = convert_to_grayscale("test.jpg", file_bytes, "image/jpeg")

    assert result_bytes == file_bytes
    assert result_type == "image/jpeg"


# TODO: why not just actually call the libraries?
def test_convert_to_grayscale_small_image(mock_grayscale_dependencies):
    """Test grayscale conversion with small valid image."""

    def mock_save(buf, format, quality=None):
        buf.write(b"small jpeg")

    mock_cv2_imdecode, mock_cv2_cvtcolor, mock_pil_fromarray = mock_grayscale_dependencies

    mock_img = MagicMock()
    mock_cv2_imdecode.return_value = mock_img
    mock_cv2_cvtcolor.return_value = MagicMock()

    mock_pil = MagicMock()
    mock_pil_fromarray.return_value = mock_pil
    mock_pil.save = mock_save

    result_bytes, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")

    assert result_type == "image/jpeg"
    assert len(result_bytes) > 0


def test_convert_to_grayscale_large_image_converts_to_pdf(mock_grayscale_dependencies):
    """Test large image converts to PDF."""
    mock_cv2_imdecode, mock_cv2_cvtcolor, mock_pil_fromarray = mock_grayscale_dependencies

    mock_cv2_imdecode.return_value = MagicMock()
    mock_cv2_cvtcolor.return_value = MagicMock()

    mock_pil = MagicMock()
    mock_pil_fromarray.return_value = mock_pil

    def save_side_effect(buf, format, quality=None):
        if format == "JPEG":
            buf.write(b"x" * (int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1))
        else:
            buf.write(b"pdf data")

    mock_pil.save = save_side_effect

    _, result_type = convert_to_grayscale("test.jpg", b"image data", "image/jpeg")

    assert result_type == "application/pdf"


def test_convert_s3_object_to_grayscale_success(s3_bucket, mocker):
    """Test successful S3 object grayscale conversion."""
    s3_bucket.put_object(Key="test.jpg", Body=b"image data", ContentType="image/jpeg")

    mock_convert = mocker.patch("documentai_api.jobs.document_processor.main.convert_to_grayscale")
    mock_convert.return_value = (b"grayscale data", "image/jpeg")

    result = convert_s3_object_to_grayscale(s3_bucket.name, "test.jpg")

    current_object = s3_bucket.Object("test.jpg")

    assert result is True
    mock_convert.assert_called_once_with("test.jpg", b"image data", "image/jpeg")

    assert current_object.content_type == "image/jpeg"
    assert current_object.get()["Body"].read() == b"grayscale data"


def test_convert_s3_object_to_grayscale_file_too_large(s3_bucket, mocker):
    """Test S3 conversion returns False when file too large."""
    s3_bucket.put_object(Key="test.jpg", Body=b"image data", ContentType="image/jpeg")

    large_bytes = b"x" * (int(ConfigDefaults.BDA_MAX_IMAGE_SIZE_BYTES.value) + 1)
    mock_convert = mocker.patch("documentai_api.jobs.document_processor.main.convert_to_grayscale")
    mock_convert.return_value = (large_bytes, "image/jpeg")

    result = convert_s3_object_to_grayscale(s3_bucket.name, "test.jpg")

    assert result is False

    # but file is still updated in S3
    current_object = s3_bucket.Object("test.jpg")
    assert current_object.get()["Body"].read() == large_bytes


def test_convert_s3_object_to_grayscale_error(s3_bucket):
    """Test S3 grayscale conversion handles errors gracefully."""
    result = convert_s3_object_to_grayscale(s3_bucket.name, "file_that_does_not_exist.jpg")

    assert result is False


def test_invoke_bda_success(input_pdf, mocker):
    """Test successful BDA invocation."""
    mock_set_status = mocker.patch(
        "documentai_api.jobs.document_processor.main.set_bda_processing_status_started"
    )

    mock_low_level_invoke = mocker.patch(
        "documentai_api.jobs.document_processor.main.invoke_bedrock_data_automation"
    )
    mock_low_level_invoke.return_value = "arn:aws:bedrock:us-east-1:123456789012:job/abc123"

    result = invoke_bda(input_pdf.bucket_name, input_pdf.key, "test.pdf")

    assert result["invocationArn"] == "arn:aws:bedrock:us-east-1:123456789012:job/abc123"
    mock_set_status.assert_called_once_with(
        object_key="test.pdf",
        bda_invocation_arn="arn:aws:bedrock:us-east-1:123456789012:job/abc123",
    )


def test_invoke_bda_failure(input_pdf, mock_invoke, mocker):
    """Test BDA invocation failure updates DDB and raises exception."""
    from botocore.exceptions import ClientError
    from tenacity import RetryError

    mock_classify = mocker.patch("documentai_api.jobs.document_processor.main.classify_as_failed")

    mock_low_level_invoke = mocker.patch(
        "documentai_api.jobs.document_processor.main.invoke_bedrock_data_automation"
    )

    # raise ClientError so retry decorator actually retries
    mock_low_level_invoke.side_effect = ClientError(
        {"Error": {"Code": "ServiceException", "Message": "BDA invocation failed"}},
        "invoke_bedrock_data_automation",
    )

    with pytest.raises(RetryError):
        invoke_bda(input_pdf.bucket_name, input_pdf.key, "test.pdf")

    mock_classify.assert_called_once()
    assert mock_classify.call_args.kwargs["object_key"] == "test.pdf"
    assert mock_classify.call_args.kwargs["error_message"] == "BDA invocation failed"


def test_main_first_time_pdf(input_pdf, mocker, ddb_doc_metadata_table, mock_invoke):
    """Test first time processing PDF (no grayscale needed)."""
    main(input_pdf.key, input_pdf.bucket_name)

    expected_object_key = "test.pdf"

    doc_meta_record = ddb_doc_metadata_table.get_item(Key={"fileName": expected_object_key})["Item"]
    assert doc_meta_record[DocumentMetadata.PROCESS_STATUS] == ProcessStatus.NOT_STARTED

    mock_invoke.assert_called_once_with(input_pdf.bucket_name, input_pdf.key, expected_object_key)


def test_main_first_time_image(input_image, mocker, ddb_doc_metadata_table, mock_invoke):
    """Test first time processing image (needs grayscale)."""
    mock_convert = mocker.patch(
        "documentai_api.jobs.document_processor.main.convert_s3_object_to_grayscale"
    )
    mock_convert.return_value = True

    main(input_image.key, input_image.bucket_name)

    expected_object_key = "test.jpg"

    doc_meta_record = ddb_doc_metadata_table.get_item(Key={"fileName": expected_object_key})["Item"]
    assert doc_meta_record[DocumentMetadata.PROCESS_STATUS] == ProcessStatus.NOT_STARTED

    mock_convert.assert_called_once_with(input_image.bucket_name, input_image.key)
    mock_invoke.assert_called_once_with(
        input_image.bucket_name, input_image.key, expected_object_key
    )


def test_main_grayscale_conversion_fails(input_image, mocker, mock_invoke):
    """Test grayscale conversion failure marks as not implemented."""
    mocker.patch("documentai_api.jobs.document_processor.main.insert_initial_ddb_record")

    mock_convert = mocker.patch(
        "documentai_api.jobs.document_processor.main.convert_s3_object_to_grayscale"
    )
    mock_convert.return_value = False

    mock_classify = mocker.patch(
        "documentai_api.jobs.document_processor.main.classify_as_not_implemented"
    )

    mock_get = mocker.patch("documentai_api.jobs.document_processor.main.get_ddb_record")
    mock_get.side_effect = [
        None,
        {DocumentMetadata.PROCESS_STATUS: ProcessStatus.PENDING_GRAYSCALE_CONVERSION},
    ]

    main(input_image.key, input_image.bucket_name)

    mock_classify.assert_called_once()
    mock_invoke.assert_not_called()


def test_main_already_processed(input_pdf, mocker, mock_invoke):
    """Test that already processed files are skipped."""
    mock_get = mocker.patch("documentai_api.jobs.document_processor.main.get_ddb_record")
    mock_get.return_value = {DocumentMetadata.PROCESS_STATUS: ProcessStatus.SUCCESS.value}

    main(input_pdf.key, input_pdf.bucket_name)

    mock_invoke.assert_not_called()


def test_main_uses_env_bucket_when_not_provided(input_pdf, mocker, mock_invoke):
    """Test bucket name defaults to environment variable."""
    main(input_pdf.key)

    mock_invoke.assert_called_once_with(input_pdf.bucket_name, input_pdf.key, "test.pdf")


def test_main_idempotent_on_duplicate_events(input_pdf, mocker, mock_invoke):
    """Test job is idempotent when receiving duplicate S3 events."""
    mock_get = mocker.patch("documentai_api.jobs.document_processor.main.get_ddb_record")
    mock_get.return_value = {DocumentMetadata.PROCESS_STATUS: ProcessStatus.STARTED.value}

    main(input_pdf.key, input_pdf.bucket_name)

    mock_invoke.assert_not_called()


def test_main_propagates_s3_metadata(input_pdf, mocker):
    """Test that job_id, trace_id, and document category are read from S3 metadata."""
    mock_insert = mocker.patch(
        "documentai_api.jobs.document_processor.main.insert_initial_ddb_record"
    )

    mock_get = mocker.patch("documentai_api.jobs.document_processor.main.get_ddb_record")
    mock_get.side_effect = [
        None,
        {DocumentMetadata.PROCESS_STATUS: ProcessStatus.NOT_STARTED.value},
    ]

    main(input_pdf.key, input_pdf.bucket_name)

    mock_insert.assert_called_once()
    call_kwargs = mock_insert.call_args.kwargs

    assert call_kwargs["job_id"] == "test-job-id"
    assert call_kwargs["trace_id"] == "test-trace-id"
    assert call_kwargs["user_provided_document_category"] == "income"
    assert call_kwargs["original_file_name"] == "original.pdf"
