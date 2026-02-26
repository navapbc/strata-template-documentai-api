import pytest

from documentai_api.utils.dates import validate_yyyymmdd_format


def test_validate_yyyymmdd_format_valid():
    """Test valid date formats."""
    validate_yyyymmdd_format("2026-02-20")  # should not raise


@pytest.mark.parametrize(
    "invalid_date",
    [
        "2026-2-20",  # missing leading zero
        "2026/02/20",  # incorrect separator
        "20-02-2026",  # incorrect order
        "2026-13-01",  # invalid month
        "2026-02-30",  # invalid day
        "not-a-date",  # invalid
        "",  # empty string
    ],
)
def test_validate_yyyymmdd_format_invalid(invalid_date):
    """Test invalid date formats raise ValueError."""
    with pytest.raises(ValueError):  # noqa: PT011
        validate_yyyymmdd_format(invalid_date)
