from datetime import UTC, datetime
from decimal import Decimal

import pytest
from freezegun import freeze_time

from documentai_api.config.constants import ProcessStatus
from documentai_api.schemas.document_metadata import DocumentMetadata
from documentai_api.utils import ddb as ddb_util
from documentai_api.utils.models import ClassificationData, InternalApiResponse
from documentai_api.utils.response_codes import ResponseCodes
from tests.helpers.assertions import assert_dict_contains


@pytest.mark.parametrize(
    ("arn", "expected_region"),
    [
        ("arn:aws:bedrock-data-automation:us-east-1:123456789012:job/abc123", "us-east-1"),
        ("arn:aws:bedrock-data-automation:eu-west-1:123456789012:job/xyz789", "eu-west-1"),
        ("invalid-arn", None),
    ],
)
def test_extract_region_from_bda_arn(arn, expected_region):
    """Test extracting AWS region from BDA ARN."""
    assert ddb_util.extract_region_from_bda_arn(arn) == expected_region


def test_get_elapsed_time_seconds():
    """Test elapsed time calculation."""
    year = datetime.now().year
    start = datetime(year, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(year, 1, 1, 12, 0, 5, 500000, tzinfo=UTC)  # 5.5 seconds later

    result = ddb_util.get_elapsed_time_seconds(start, end)

    assert result == Decimal("5.5")
    assert isinstance(result, Decimal)


def test_calculate_bda_processing_times(ddb_doc_metadata_table):
    """Test BDA processing time calculation."""
    year = datetime.now().year
    created_at = datetime(year, 1, 1, 12, 0, 0, tzinfo=UTC)
    bda_started_at = datetime(year, 1, 1, 12, 0, 5, tzinfo=UTC)
    completion_time = datetime(year, 1, 1, 12, 0, 15, tzinfo=UTC)

    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-file",
        DocumentMetadata.CREATED_AT: created_at.isoformat(),
        DocumentMetadata.BDA_STARTED_AT: bda_started_at.isoformat(),
    }

    ddb_doc_metadata_table.put_item(Item=ddb_record)

    result = ddb_util.calculate_bda_processing_times("test-file", completion_time)

    assert result.total_processing_time_seconds == Decimal("15.0")
    assert result.bda_processing_time_seconds == Decimal("10.0")


@freeze_time("2026-01-01 12:00:10+00:00")
def test_calculate_wait_time(ddb_doc_metadata_table):
    """Test BDA wait time calculation."""
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-file",
        DocumentMetadata.CREATED_AT: created_at.isoformat(),
    }

    ddb_doc_metadata_table.put_item(Item=ddb_record)

    wait_time = ddb_util._calculate_wait_time("test-file")
    assert wait_time == Decimal("10.0")


@pytest.mark.parametrize(
    (
        "field_confidence_scores",
        "field_empty_list",
        "expected_count",
        "expected_non_empty",
        "expected_avg",
    ),
    [
        (None, None, 0, 0, None),
        ([], None, 0, 0, None),
        ([{"field1": 0.95}, {"field2": 0.85}], None, 2, 2, 0.9),
        ([{"field1": 0.95}, {"field2": 0.85}, {"field3": 0.75}], ["field3"], 3, 2, 0.9),
        ([{"field1": 0.8}], ["field1"], 1, 0, None),
    ],
)
def test_calculate_field_metrics(
    field_confidence_scores, field_empty_list, expected_count, expected_non_empty, expected_avg
):
    """Test field metrics calculation."""
    data = ClassificationData(
        field_confidence_scores=field_confidence_scores,
        field_empty_list=field_empty_list,
    )

    metrics = ddb_util._calculate_field_metrics(data)

    assert metrics.field_count == expected_count
    assert metrics.field_count_not_empty == expected_non_empty
    assert metrics.field_not_empty_avg_confidence == pytest.approx(expected_avg)


@pytest.mark.parametrize("has_bda_started_at", [True, False])
@freeze_time("2026-01-01 12:00:15+00:00")
def test_build_completion_timing(has_bda_started_at, ddb_doc_metadata_table, mocker):
    """Test completion timing updates."""
    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-file",
        DocumentMetadata.CREATED_AT: datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC).isoformat(),
    }

    if has_bda_started_at:
        ddb_record[DocumentMetadata.BDA_STARTED_AT] = datetime(
            2026, 1, 1, 12, 0, 5, tzinfo=UTC
        ).isoformat()

    ddb_doc_metadata_table.put_item(Item=ddb_record)

    mock_get_modified = mocker.patch("documentai_api.utils.ddb.s3_service.get_last_modified_at")
    mock_get_modified.return_value = datetime(2026, 1, 1, 12, 0, 15, tzinfo=UTC)

    bda_output_s3_uri = "s3://bucket/key/job_metadata.json" if has_bda_started_at else None
    updates, values = ddb_util._build_completion_timing("test-file", bda_output_s3_uri)

    if has_bda_started_at:
        assert any(DocumentMetadata.BDA_COMPLETED_AT in u for u in updates)
        assert any(DocumentMetadata.PROCESSED_DATE in u for u in updates)
        assert ":bdaCompletedAt" in values
        assert ":processedDate" in values
        assert values[":totalProcessingTime"] == Decimal("15.0")
        assert values[":bdaProcessingTime"] == Decimal("10.0")
        mock_get_modified.assert_called_once_with("bucket", "key/job_metadata.json")
    else:
        assert updates == []
        assert values == {}
        mock_get_modified.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        ProcessStatus.STARTED,
        ProcessStatus.SUCCESS,
        ProcessStatus.FAILED,
        ProcessStatus.PENDING_GRAYSCALE_CONVERSION,
    ],
)
@freeze_time("2026-01-01 12:00:10+00:00")
def test_build_timing_updates(status, ddb_doc_metadata_table, mocker):
    """Test timing updates for different statuses."""
    ddb_record = {
        DocumentMetadata.FILE_NAME: "test-file",
        DocumentMetadata.CREATED_AT: datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC).isoformat(),
    }

    if status in [ProcessStatus.SUCCESS, ProcessStatus.FAILED]:
        ddb_record[DocumentMetadata.BDA_STARTED_AT] = datetime(
            2026, 1, 1, 12, 0, 5, tzinfo=UTC
        ).isoformat()

    ddb_doc_metadata_table.put_item(Item=ddb_record)

    mock_get_modified = mocker.patch("documentai_api.utils.ddb.s3_service.get_last_modified_at")

    bda_output_s3_uri = (
        "s3://bucket/key/result.json"
        if status in [ProcessStatus.SUCCESS, ProcessStatus.FAILED]
        else None
    )

    if bda_output_s3_uri:
        mock_get_modified.return_value = datetime(2026, 1, 1, 12, 0, 10, tzinfo=UTC)

    updates, values = ddb_util._build_timing_updates("test-file", status, bda_output_s3_uri)

    if status == ProcessStatus.STARTED:
        assert DocumentMetadata.BDA_STARTED_AT in updates
        assert DocumentMetadata.BDA_WAIT_TIME_SECONDS in updates
        assert DocumentMetadata.BDA_COMPLETED_AT not in updates
        assert DocumentMetadata.PROCESSED_DATE not in updates
        assert values[":bdaWaitTimeSeconds"] == Decimal("10.0")
    elif status in [ProcessStatus.SUCCESS, ProcessStatus.FAILED]:
        assert DocumentMetadata.BDA_COMPLETED_AT in updates
        assert DocumentMetadata.PROCESSED_DATE in updates
        assert DocumentMetadata.BDA_STARTED_AT not in updates
        assert DocumentMetadata.BDA_WAIT_TIME_SECONDS not in updates
        assert values[":totalProcessingTime"] == Decimal("10.0")
        assert values[":bdaProcessingTime"] == Decimal("5.0")
    else:
        assert updates == ""
        assert values == {}


@pytest.mark.parametrize(
    ("internal_api_response", "v1_api_response", "bda_invocation_arn", "error_message"),
    [
        # all parameters populated. tests all 'if' paths
        (
            InternalApiResponse(
                validation_passed=True,
                document_category="income",
                matched_document_class="paystub",
                response_code=ResponseCodes.SUCCESS,
                response_message="Success",
            ),
            {"result": 200},
            "arn:aws:bedrock-data-automation:us-east-1:123:job/abc",
            "Test error message",
        ),
        # all parameters None/empty tests 'if' paths not executed
        (None, None, None, None),
    ],
)
def test_build_update_expression(
    internal_api_response, v1_api_response, bda_invocation_arn, error_message
):
    """Test update expression building."""
    data = ClassificationData(
        bda_output_s3_uri="s3://bucket/key",
        matched_blueprint_name="test-blueprint",
        matched_blueprint_confidence=0.95,
    )

    expr, values = ddb_util._build_update_expression(
        status=ProcessStatus.SUCCESS.value,
        data=data,
        internal_api_response=internal_api_response,
        v1_api_response=v1_api_response,
        bda_invocation_arn=bda_invocation_arn,
        error_message=error_message,
    )

    # confirm base fields are always present
    assert "SET" in expr
    assert DocumentMetadata.PROCESS_STATUS in expr
    assert ":processStatus" in values
    assert values[":processStatus"] == ProcessStatus.SUCCESS.value

    # verify attributes exist in update if populated, else attribute should not be present
    if internal_api_response:
        assert DocumentMetadata.RESPONSE_JSON in expr
        assert ":responseJson" in values
        assert DocumentMetadata.RESPONSE_CODE in expr
        assert ":responseCode" in values
    else:
        assert DocumentMetadata.RESPONSE_JSON not in expr
        assert ":responseJson" not in values
        assert DocumentMetadata.RESPONSE_CODE not in expr
        assert ":responseCode" not in values

    if v1_api_response:
        assert DocumentMetadata.V1_API_RESPONSE_JSON in expr
        assert ":v1ResponseJson" in values
    else:
        assert DocumentMetadata.V1_API_RESPONSE_JSON not in expr
        assert ":v1ResponseJson" not in values

    if bda_invocation_arn:
        assert DocumentMetadata.BDA_INVOCATION_ARN in expr
        assert ":bdaInvocationArn" in values
        assert DocumentMetadata.BDA_REGION_USED in expr
        assert ":bdaRegion" in values
    else:
        assert DocumentMetadata.BDA_INVOCATION_ARN not in expr
        assert ":bdaInvocationArn" not in values
        assert DocumentMetadata.BDA_REGION_USED not in expr
        assert ":bdaRegion" not in values

    if error_message:
        assert DocumentMetadata.ERROR_MESSAGE in expr
        assert ":errorMessage" in values
    else:
        assert DocumentMetadata.ERROR_MESSAGE not in expr
        assert ":errorMessage" not in values


def test_execute_ddb_update(ddb_doc_metadata_table):
    object_key = "table-key"
    item = {DocumentMetadata.FILE_NAME: object_key, "foo": "bar"}
    ddb_doc_metadata_table.put_item(Item=item)

    update_expression = "SET foo = :status"
    expression_values = {":status": "test"}

    ddb_util._execute_ddb_update(object_key, update_expression, expression_values)

    doc_meta_record = ddb_doc_metadata_table.get_item(Key={"fileName": object_key})["Item"]
    assert doc_meta_record["foo"] == "test"


@pytest.mark.parametrize("user_provided_document_category", ["income", None])
def test_get_user_provided_document_category(
    ddb_doc_metadata_table, user_provided_document_category
) -> str:
    item = {
        DocumentMetadata.FILE_NAME: "test-file",
        DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY: user_provided_document_category,
    }
    ddb_doc_metadata_table.put_item(Item=item)

    category = ddb_util.get_user_provided_document_category("test-file")
    assert category == user_provided_document_category


def test_get_ddb_record(ddb_doc_metadata_table):
    item = {
        DocumentMetadata.FILE_NAME: "test-file",
        DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY: "income",
        DocumentMetadata.PROCESS_STATUS: "completed",
    }
    ddb_doc_metadata_table.put_item(Item=item)

    ddb_record = ddb_util.get_ddb_record("test-file")

    for k, v in item.items():
        assert ddb_record[k] == v


def test_get_ddb_by_job_id(ddb_doc_metadata_table):
    """Test getting DDB record by job ID."""
    job_id = "job-123"
    file_name = "test-file"
    ddb_record = {DocumentMetadata.JOB_ID: job_id, DocumentMetadata.FILE_NAME: file_name}
    ddb_doc_metadata_table.put_item(Item=ddb_record)

    result = ddb_util.get_ddb_by_job_id(job_id)

    for k, v in ddb_record.items():
        assert result[k] == v


@pytest.mark.parametrize(
    ("status", "has_timing"),
    [
        (ProcessStatus.SUCCESS.value, True),
        (ProcessStatus.STARTED.value, True),
        (ProcessStatus.NOT_STARTED.value, False),
    ],
)
def test_update_ddb(status, has_timing, ddb_doc_metadata_table, mocker):
    """Test DDB update."""
    import json

    internal_response = InternalApiResponse(
        validation_passed=True,
        document_category="income",
        matched_document_class="paystub",
        response_code=ResponseCodes.SUCCESS,
        response_message="Success",
    )
    data = ClassificationData(matched_document_class="paystub")

    mock_timing = mocker.patch("documentai_api.utils.ddb._build_timing_updates")
    mock_timing.return_value = ("timing = :t", {":t": "val"}) if has_timing else ("", {})

    mock_v1 = mocker.patch("documentai_api.utils.ddb.build_v1_api_response")
    mock_v1.return_value = {"status": "completed"}

    object_key = "test-file"

    ddb_util.update_ddb(object_key, status, internal_response, data)

    item = ddb_doc_metadata_table.get_item(Key={"fileName": object_key})["Item"]
    assert item[DocumentMetadata.PROCESS_STATUS] == status
    assert item[DocumentMetadata.V1_API_RESPONSE_JSON] == json.dumps(mock_v1.return_value)

    if has_timing:
        assert item["timing"] == "val"


def test_insert_ddb(ddb_doc_metadata_table, mocker):
    """Test DDB insert with all fields."""
    mock_raw_metrics = mocker.MagicMock()
    mock_raw_metrics.to_json_dict.return_value = {"raw": "data"}
    mock_normalized_metrics = mocker.MagicMock()
    mock_normalized_metrics.to_json_dict.return_value = {"normalized": "data"}

    internal_response = InternalApiResponse(
        validation_passed=True,
        document_category="income",
        matched_document_class="paystub",
        response_code=ResponseCodes.SUCCESS,
        response_message="Success",
    )

    object_key = "test-file"

    ddb_util.insert_ddb(
        object_key=object_key,
        original_file_name="original-test.pdf",
        user_provided_document_category="income",
        process_status=ProcessStatus.NOT_STARTED.value,
        internal_api_response=internal_response,
        file_size_bytes=1024,
        content_type="application/pdf",
        pages_detected=5,
        job_id="job-123",
        trace_id="trace-456",
        is_password_protected=True,
        is_document_blurry=False,
        document_profile_raw_metrics=mock_raw_metrics,
        document_profile_normalized_metrics=mock_normalized_metrics,
        overall_blur_score=0.85,
    )

    item = ddb_doc_metadata_table.get_item(Key={"fileName": object_key})["Item"]

    # base fields
    assert item[DocumentMetadata.FILE_NAME] == "test-file"
    assert item[DocumentMetadata.USER_PROVIDED_DOCUMENT_CATEGORY] == "income"
    assert item[DocumentMetadata.PROCESS_STATUS] == ProcessStatus.NOT_STARTED.value
    assert item[DocumentMetadata.FILE_SIZE_BYTES] == 1024
    assert item[DocumentMetadata.CONTENT_TYPE] == "application/pdf"
    assert DocumentMetadata.CREATED_AT in item
    assert DocumentMetadata.UPDATED_AT in item

    # optional fields
    assert item[DocumentMetadata.PAGES_DETECTED] == 5
    assert item[DocumentMetadata.JOB_ID] == "job-123"
    assert item[DocumentMetadata.TRACE_ID] == "trace-456"
    assert item[DocumentMetadata.IS_PASSWORD_PROTECTED] is True
    assert item[DocumentMetadata.IS_DOCUMENT_BLURRY] is False
    assert DocumentMetadata.RESPONSE_JSON in item
    assert DocumentMetadata.DOCUMENT_METRICS_RAW in item
    assert DocumentMetadata.DOCUMENT_METRICS_NORMALIZED in item
    assert item[DocumentMetadata.OVERALL_BLUR_SCORE] == Decimal("0.85")


@pytest.mark.parametrize(
    (
        "user_provided_document_category",
        "content_type",
        "is_password_protected",
        "is_blurry",
        "expected_status",
        "has_internal_response",
    ),
    [
        ("income", "image/bmp", False, False, ProcessStatus.NOT_IMPLEMENTED, True),
        ("income", "application/pdf", True, False, ProcessStatus.PASSWORD_PROTECTED, True),
        ("income", "application/pdf", False, True, ProcessStatus.BLURRY_DOCUMENT_DETECTED, True),
        ("income", "image/jpeg", False, False, ProcessStatus.PENDING_GRAYSCALE_CONVERSION, False),
        ("income", "application/pdf", False, False, ProcessStatus.NOT_STARTED, False),
        (None, "application/pdf", False, False, ProcessStatus.NOT_STARTED, False),
    ],
)
def test_insert_initial_ddb_record(
    ddb_doc_metadata_table,
    set_ddb_doc_metadata_table_env_vars,
    s3_bucket,
    user_provided_document_category,
    content_type,
    is_password_protected,
    is_blurry,
    expected_status,
    has_internal_response,
    mocker,
):
    import json

    from documentai_api.utils.document_detector import (
        DocumentProfile,
        QualityMetricsNormalized,
        QualityMetricsRaw,
    )

    mock_get_internal_api_response = mocker.patch(
        "documentai_api.utils.ddb.get_internal_api_response"
    )
    if has_internal_response:
        mock_get_internal_api_response.return_value = InternalApiResponse(
            validation_passed=True,
            document_category="income",
            matched_document_class="paystub",
            response_code=ResponseCodes.SUCCESS,
            response_message="Success",
        )
    else:
        mock_get_internal_api_response.return_value = None

    mock_document_profile = DocumentProfile(
        page_count=1,
        raw_metrics=QualityMetricsRaw(
            fft_score=0.0,
            edge_score=0.0,
            laplacian_variance=0.0,
            local_contrast=0.0,
            sobel_score=0.0,
            noise_stddev=0.0,
            motion_blur_score=0.0,
        ),
        normalized_metrics=QualityMetricsNormalized(
            fft_score=1.0,
            edge_score=1.0,
            laplacian_variance=1.0,
            local_contrast=1.0,
            sobel_score=1.0,
            noise_stddev=1.0,
        ),
        normalization_ranges=None,
        overall_blur_score=0.0,
        is_blurry=is_blurry,
        is_multipage=False,
        is_password_protected=is_password_protected,
    )

    mock_document_detector_class = mocker.patch("documentai_api.utils.ddb.DocumentDetector")
    mock_document_detector_instance = mocker.MagicMock()
    mock_document_detector_class.return_value = mock_document_detector_instance
    mock_document_detector_instance.get_document_profile.return_value = mock_document_profile
    mock_document_detector_instance.is_multidoc_in_single_page.return_value = False

    s3_object = s3_bucket.put_object(
        Key="input/test-file",
        Body=b"bytes",
        ContentType=content_type,
        Metadata={
            "job-id": "test-job-id",
            "trace-id": "test-trace-id",
            "original-file-name": "original-test.pdf",
        },
    )

    ddb_util.insert_initial_ddb_record(
        source_bucket_name=s3_object.bucket_name,
        source_object_key=s3_object.key,
        original_file_name="original-test.pdf",
        ddb_key="test-file",
        user_provided_document_category=user_provided_document_category,
        job_id="test-job-id",
        trace_id="test-trace-id",
    )

    mock_document_detector_instance.get_document_profile.assert_called_once_with(
        b"bytes", s3_object.key
    )

    doc_meta_record = ddb_doc_metadata_table.get_item(Key={"fileName": "test-file"})["Item"]

    expected_record = {
        "fileName": "test-file",
        "originalFileName": "original-test.pdf",
        "userProvidedDocumentCategory": user_provided_document_category or "unknown",
        "processStatus": expected_status.value,
        "fileSizeBytes": Decimal(5),
        "contentType": content_type,
        "jobId": "test-job-id",
        "traceId": "test-trace-id",
        "isDocumentBlurry": is_blurry,
        "isPasswordProtected": is_password_protected,
        "pagesDetected": Decimal(1),
        "documentMetricsRaw": json.dumps(mock_document_profile.raw_metrics.to_json_dict()),
        "documentMetricsNormalized": json.dumps(
            mock_document_profile.normalized_metrics.to_json_dict()
        ),
        "overallBlurScore": Decimal(0),
    }

    assert_dict_contains(doc_meta_record, expected_record)

    assert "createdAt" in doc_meta_record
    assert "updatedAt" in doc_meta_record

    if has_internal_response:
        assert doc_meta_record["responseJson"] == json.dumps(
            mock_get_internal_api_response.return_value.__dict__
        )
    else:
        assert "responseJson" not in doc_meta_record


def test_set_bda_processing_status_started(mocker):
    """Test setting BDA status to started."""
    mock_update = mocker.patch("documentai_api.utils.ddb.update_ddb")

    ddb_util.set_bda_processing_status_started("test-file", "arn:aws:bda:us-east-1:123:job/1")

    mock_update.assert_called_once_with(
        object_key="test-file",
        status=ProcessStatus.STARTED,
        internal_api_response=None,
        bda_invocation_arn="arn:aws:bda:us-east-1:123:job/1",
    )


def test_set_bda_processing_status_not_started(mocker):
    """Test setting BDA status to not started."""
    mock_update = mocker.patch("documentai_api.utils.ddb.update_ddb")

    ddb_util.set_bda_processing_status_not_started("test-file")

    mock_update.assert_called_once_with(
        object_key="test-file",
        status=ProcessStatus.NOT_STARTED,
        internal_api_response=None,
    )


# test all classify_as* methods - classify_as_success, classify_as_failed, etc.
# the structure is essentially the identical, test using parameterization rather
# than repeating boilerplate code each time
@pytest.mark.parametrize(
    ("function", "response_code", "status", "matched_document_class", "error_msg"),
    [
        (
            ddb_util.classify_as_success,
            ResponseCodes.SUCCESS,
            ProcessStatus.SUCCESS,
            "paystub",
            None,
        ),
        (
            ddb_util.classify_as_failed,
            ResponseCodes.INTERNAL_PROCESSING_ERROR,
            ProcessStatus.FAILED,
            None,
            "Test error",
        ),
        (
            ddb_util.classify_as_not_implemented,
            ResponseCodes.DOCUMENT_TYPE_NOT_IMPLEMENTED,
            ProcessStatus.SUCCESS,
            None,
            None,
        ),
        (
            ddb_util.classify_as_no_document_detected,
            ResponseCodes.NO_DOCUMENT_DETECTED,
            ProcessStatus.NO_DOCUMENT_DETECTED,
            None,
            None,
        ),
        (
            ddb_util.classify_as_no_custom_blueprint_matched,
            ResponseCodes.DOCUMENT_TYPE_NOT_IMPLEMENTED,
            ProcessStatus.NO_CUSTOM_BLUEPRINT_MATCHED,
            None,
            None,
        ),
    ],
)
def test_classify_functions(
    function, response_code, status, matched_document_class, error_msg, mocker
):
    """Test all classify_as_* functions."""
    data = ClassificationData(matched_document_class="paystub")

    mock_get_response = mocker.patch("documentai_api.utils.ddb.get_internal_api_response")
    mock_update = mocker.patch("documentai_api.utils.ddb.update_ddb")

    # all classify functions require an object key and classification data
    args = ["test-file", data]

    # classify as failure requires an error message as the second argument
    if error_msg:
        args.insert(1, error_msg)

    # classify as success requires response_code as the second argument
    elif response_code == ResponseCodes.SUCCESS:
        args.insert(1, response_code)

    function(*args)

    mock_get_response.assert_called_once_with(
        object_key="test-file",
        response_code=response_code,
        matched_document_class=matched_document_class,
    )

    expected_call = {
        "object_key": "test-file",
        "status": status,
        "internal_api_response": mock_get_response.return_value,
        "data": data,
    }

    if error_msg:
        expected_call["error_message"] = error_msg

    mock_update.assert_called_once_with(**expected_call)
