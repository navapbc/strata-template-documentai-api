from dataclasses import dataclass

import pytest
from config.constants import (
    BDA_JOB_STATUS_COMPLETED,
    BDA_JOB_STATUS_FAILED,
    BDA_JOB_STATUS_RUNNING,
)
from utils import bda as bda_util


@dataclass
class BdaJobStatusTestCase:
    status: str
    expect_is_running: bool
    expect_is_failed: bool
    expect_is_completed: bool


def generate_bda_status_test_cases():
    """Generate comprehensive test cases for all BDA status checks"""
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


@pytest.mark.skip(reason="Pending sample BDA output files")
def test_extract_fields_recursive():
    pass


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


@pytest.mark.skip(reason="Pending sample BDA output files")
def test_get_text_from_standard_blueprint():
    pass


@pytest.mark.skip(reason="Pending sample BDA output files")
def test_extract_field_values_from_bda_results():
    pass


@pytest.mark.skip(reason="Pending sample BDA output files")
def test_extract_field_metadata_from_bda_results():
    pass
