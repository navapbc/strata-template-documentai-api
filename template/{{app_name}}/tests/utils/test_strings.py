import pytest

from documentai_api.utils import strings as string_util


@pytest.mark.parametrize(
    ("snake_case", "expected_camel"),
    [
        ("user_provided_document_category", "userProvidedDocumentCategory"),
        ("job_id", "jobId"),
        ("trace_id", "traceId"),
        ("single", "single"),
        ("already_camelCase", "alreadyCamelcase"),
    ],
)
def test_snake_to_camel(snake_case, expected_camel):
    assert string_util.snake_to_camel(snake_case) == expected_camel
