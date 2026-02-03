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
    for status in BDA_JOB_STATUS_RUNNING:
        test_cases.append(BdaJobStatusTestCase(status, True, False, False))

    # failed statuses
    for status in BDA_JOB_STATUS_FAILED:
        test_cases.append(BdaJobStatusTestCase(status, False, True, False))

    # completed statuses
    for status in BDA_JOB_STATUS_COMPLETED:
        test_cases.append(BdaJobStatusTestCase(status, False, False, True))

    # invalid/bogus statuses
    for status in ["UNKNOWN", "INVALID", "", None]:
        test_cases.append(BdaJobStatusTestCase(status, False, False, False))

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


def test_extract_fields_recursive():
    data = {
        "name": {"confidence": 0.95, "value": "John"},
        "age": {"confidence": 0.80, "value": "30"},
        "empty_field": {"confidence": 0.70, "value": ""},
        # only dict values with confidence or value fields are extracted. non-dict
        # values are ignored. include scalars as a negative test.
        "string_value": "not_a_dict",
        "number_value": 123,
        "person": {
            "address": {
                "street": {"confidence": 0.85, "value": "123 Main St"},
                "city": {"confidence": 0.90, "value": ""},
            }
        },
    }
    confidence_scores = []
    empty_fields = []
    field_confidence_map_list = []
    field_values = {}

    bda_util._extract_fields_recursive(
        data, "", confidence_scores, empty_fields, field_confidence_map_list, field_values
    )

    assert len(confidence_scores) == 3
    assert "empty_field" in empty_fields
    assert "person.address.city" in empty_fields
    assert field_values["name"] == "John"
    assert field_values["person.address.street"] == "123 Main St"
    assert "string_value" not in field_values
    assert "number_value" not in field_values


@pytest.mark.parametrize(
    "field_data,expected_confidence,expected_is_empty",
    [
        ({"confidence": 0.95, "value": "John"}, 0.95, False),
        ({"confidence": 0.80, "value": ""}, 0.80, True),
        ({"value": "Test"}, 0, False),  # missing confidence
        ({"confidence": 0.50}, 0.50, True),  # missing value
    ],
)
def test_process_single_field(field_data, expected_confidence, expected_is_empty):
    result = bda_util._process_single_field("field", field_data)
    assert result.confidence == expected_confidence
    assert result.is_empty == expected_is_empty


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
