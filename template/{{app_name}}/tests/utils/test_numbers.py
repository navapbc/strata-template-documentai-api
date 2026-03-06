import pytest

from documentai_api.utils.numbers import normalize


@pytest.mark.parametrize(
    ("value", "min_val", "max_val", "expected"),
    [
        (50, 0, 100, 0.5),
        (0, 0, 100, 0.0),
        (100, 0, 100, 1.0),
        (-10, 0, 100, 0.0),  # resolves to 0
        (150, 0, 100, 1.0),  # resolves to 1
    ],
)
def test_normalize(value, min_val, max_val, expected):
    assert normalize(value, min_val, max_val) == expected
