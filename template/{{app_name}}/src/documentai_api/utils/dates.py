"""Date utilities."""

import re
from datetime import datetime


def validate_yyyymmdd_format(date_str: str) -> datetime:
    """Validate date format is YYYY-MM-DD.

    Args:
        date_str: Date string to validate

    Returns:
        datetime object

    Raises:
        ValueError: If date format is invalid
    """
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD.")

    return datetime.strptime(date_str, "%Y-%m-%d")
