from unittest.mock import MagicMock, patch

from documentai_api.utils import bda_invoker as bda_invoker_util


def test_invoke_bedrock_data_automation_single_page(runtime_required_env):
    bda_invocation_arn = "arn:aws:invocation:123"

    with (
        patch(
            "documentai_api.utils.bda_invoker.AWSClientFactory.get_bda_runtime_client"
        ) as mock_get_bda_client,
        patch("documentai_api.services.s3.get_file_bytes") as mock_get_file_bytes,
        patch("documentai_api.utils.document_detector.DocumentDetector") as mock_detector_class,
    ):
        mock_bda = MagicMock()
        mock_bda.invoke_data_automation_async.return_value = {"invocationArn": bda_invocation_arn}
        mock_get_bda_client.return_value = mock_bda

        mock_get_file_bytes.return_value = b"file_content"

        mock_detector = MagicMock()
        mock_detector.get_page_count.return_value = 3
        mock_detector_class.return_value = mock_detector

        result = bda_invoker_util.invoke_bedrock_data_automation("test-bucket", "test.pdf")

        assert result == bda_invocation_arn
        mock_bda.invoke_data_automation_async.assert_called_once()


def test_invoke_bedrock_data_automation_document_truncation(runtime_required_env):
    bda_invocation_arn = "arn:aws:invocation:123"

    with (
        patch(
            "documentai_api.utils.bda_invoker.AWSClientFactory.get_bda_runtime_client"
        ) as mock_get_bda_client,
        patch("documentai_api.services.s3.get_file_bytes") as mock_get_file_bytes,
        patch("documentai_api.services.s3.put_object") as mock_put_object,
        patch("documentai_api.utils.document_detector.DocumentDetector") as mock_detector_class,
    ):
        mock_bda = MagicMock()
        mock_bda.invoke_data_automation_async.return_value = {"invocationArn": bda_invocation_arn}
        mock_get_bda_client.return_value = mock_bda

        mock_get_file_bytes.return_value = b"file_content"

        mock_detector = MagicMock()
        mock_detector.get_page_count.return_value = 10
        mock_detector.truncate_to_pages.return_value = b"truncated_content"
        mock_detector_class.return_value = mock_detector

        result = bda_invoker_util.invoke_bedrock_data_automation("test-bucket", "test.pdf")

        assert result == bda_invocation_arn
        mock_detector.truncate_to_pages.assert_called_once_with(b"file_content", max_pages=5)
        mock_put_object.assert_called_once_with(
            bucket="test-bucket", key="test_truncated.pdf", body=b"truncated_content"
        )
