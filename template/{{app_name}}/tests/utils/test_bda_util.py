from dataclasses import dataclass

import pytest

from documentai_api.config.constants import (
    BDA_JOB_STATUS_COMPLETED,
    BDA_JOB_STATUS_FAILED,
    BDA_JOB_STATUS_RUNNING,
)
from documentai_api.utils import bda as bda_util


@dataclass
class BdaJobStatusTestCase:
    status: str
    expect_is_running: bool
    expect_is_failed: bool
    expect_is_completed: bool


def generate_bda_status_test_cases():
    """Generate comprehensive test cases for all BDA status checks."""
    test_cases = []

    # running statuses
    test_cases.extend(
        BdaJobStatusTestCase(status, True, False, False) for status in BDA_JOB_STATUS_RUNNING
    )

    # failed statuses
    test_cases.extend(
        BdaJobStatusTestCase(status, False, True, False) for status in BDA_JOB_STATUS_FAILED
    )

    # completed statuses
    test_cases.extend(
        BdaJobStatusTestCase(status, False, False, True) for status in BDA_JOB_STATUS_COMPLETED
    )

    # invalid/bogus statuses
    test_cases.extend(
        BdaJobStatusTestCase(status, False, False, False)
        for status in ["UNKNOWN", "INVALID", "", None]
    )

    return test_cases


@pytest.mark.parametrize("test_case", generate_bda_status_test_cases())
def test_is_bda_job_running(test_case):
    assert bda_util.is_bda_job_running(test_case.status) == test_case.expect_is_running


@pytest.mark.parametrize("test_case", generate_bda_status_test_cases())
def test_is_bda_job_failed(test_case):
    assert bda_util.is_bda_job_failed(test_case.status) == test_case.expect_is_failed


@pytest.mark.parametrize("test_case", generate_bda_status_test_cases())
def test_is_bda_job_completed(test_case):
    assert bda_util.is_bda_job_completed(test_case.status) == test_case.expect_is_completed


def test_get_text_from_standard_blueprint_document_modality():
    bda_result = {
        "metadata": {"semantic_modality": "DOCUMENT"},
        "pages": [{"representation": {"text": "  Sample document text  "}}],
    }
    text = bda_util.get_text_from_standard_blueprint(bda_result)
    assert text == "Sample document text"


def test_get_text_from_standard_blueprint_image_modality():
    bda_result = {
        "metadata": {"semantic_modality": "IMAGE"},
        "image": {
            "text_words": [
                {"text": "Hello"},
                {"text": "World"},
                {"text": ""},
            ]
        },
    }
    text = bda_util.get_text_from_standard_blueprint(bda_result)
    assert text == "Hello World"


def test_extract_field_values_from_bda_results():
    bda_result = {
        "explainability_info": [
            {
                "name": {"confidence": 0.95, "value": "John"},
                "email": {"confidence": 0.85, "value": "john@example.com"},
            }
        ]
    }
    metadata, field_values = bda_util.extract_field_values_from_bda_results(bda_result)

    assert len(metadata.confidence_scores) == 2
    assert len(metadata.empty_fields) == 0
    assert field_values["name"] == "John"
    assert field_values["email"] == "john@example.com"

    # confirm extract_field_metadata_from_bda_results wrapper returns same metadata
    metadata_only = bda_util.extract_field_metadata_from_bda_results(bda_result)
    assert metadata_only.confidence_scores == metadata.confidence_scores
    assert metadata_only.empty_fields == metadata.empty_fields
