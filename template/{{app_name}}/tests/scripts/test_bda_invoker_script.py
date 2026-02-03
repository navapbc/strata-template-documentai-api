"""Tests for scripts/bda_invoker.py."""

from unittest.mock import patch

import pytest
from documentai_api.scripts.bda_invoker import main


@pytest.fixture(autouse=True)
def mock_env():
    """Mock environment variables for all tests."""
    with patch.dict("os.environ", {"DDE_INPUT_LOCATION": "s3://test-bucket"}):
        yield


def test_main_success():
    """Test successful BDA invocation."""
    with (patch("documentai_api.scripts.bda_invoker.get_ddb_record") as mock_get_ddb_record,
        patch("documentai_api.scripts.bda_invoker.invoke_bedrock_data_automation") as mock_invoke_bda,
            patch("documentai_api.scripts.bda_invoker.set_bda_processing_status_started")):
                mock_get_ddb_record.return_value = {"processStatus": "not_started"}
                mock_invoke_bda.return_value = "arn:aws:bedrock:us-east-1:123456789012:job/abc123"

                result = main("test-file.pdf")

    assert result["statusCode"] == 200
    assert result["invocationArn"] == "arn:aws:bedrock:us-east-1:123456789012:job/abc123"
    assert "skipped" not in result


def test_main_with_explicit_bucket():
    """Test BDA invocation with explicit bucket name."""
    with (patch("documentai_api.scripts.bda_invoker.get_ddb_record") as mock_get_ddb_record,
         patch("documentai_api.scripts.bda_invoker.invoke_bedrock_data_automation") as mock_invoke_bda,
             patch("documentai_api.scripts.bda_invoker.set_bda_processing_status_started")):
                mock_get_ddb_record.return_value = {"processStatus": "not_started"}
                mock_invoke_bda.return_value = "arn:aws:bedrock:us-east-1:123456789012:job/abc123"

                result = main("test-file.pdf", bucket_name="custom-bucket")

    assert result["statusCode"] == 200
    mock_invoke_bda.assert_called_once_with("custom-bucket", "test-file.pdf")


@pytest.mark.parametrize(
    "process_status",
    ["processing", "completed", "failed"],
)
def test_main_skips_already_processed_files(process_status):
    """Test that files with non-not_started status are skipped."""
    with patch("documentai_api.scripts.bda_invoker.get_ddb_record") as mock_get_ddb_record:
        mock_get_ddb_record.return_value = {"processStatus": process_status}

        result = main("test-file.pdf")

    assert result["statusCode"] == 200
    assert result["skipped"] is True
    assert process_status in result["reason"]


def test_main_bypass_ddb_status_check():
    """Test bypassing DDB status check."""
    with (patch("documentai_api.scripts.bda_invoker.get_ddb_record") as mock_get_ddb_record,
         patch("documentai_api.scripts.bda_invoker.invoke_bedrock_data_automation") as mock_invoke_bda,
             patch("documentai_api.scripts.bda_invoker.set_bda_processing_status_started")):
                mock_invoke_bda.return_value = "arn:aws:bedrock:us-east-1:123456789012:job/abc123"

                result = main("test-file.pdf", bypass_ddb_status_check=True)

    # should not check ddb record
    mock_get_ddb_record.assert_not_called()
    assert result["statusCode"] == 200


def test_main_no_ddb_record_found():
    """Test error when DDB record not found."""
    with patch("documentai_api.scripts.bda_invoker.get_ddb_record") as mock_get_ddb_record:
        mock_get_ddb_record.side_effect = ValueError("Record not found")

        with pytest.raises(ValueError, match="Record not found"):
            main("test-file.pdf")


def test_main_bda_invocation_fails():
    """Test BDA invocation failure updates DDB and raises exception."""
    with (patch("documentai_api.scripts.bda_invoker.get_ddb_record") as mock_get_ddb_record,
         patch("documentai_api.scripts.bda_invoker.invoke_bedrock_data_automation") as mock_invoke_bda,
             patch("documentai_api.scripts.bda_invoker.classify_as_failed") as mock_classify_as_failed):
                mock_get_ddb_record.return_value = {"processStatus": "not_started"}
                mock_invoke_bda.side_effect = Exception("BDA service error")

                with pytest.raises(Exception, match="BDA service error"):
                    main("test-file.pdf")

                # verify failure was recorded in ddb
                mock_classify_as_failed.assert_called_once()
                assert mock_classify_as_failed.call_args.kwargs["object_key"] == "test-file.pdf"
                assert "BDA invocation failed" in mock_classify_as_failed.call_args.kwargs["error_message"]
