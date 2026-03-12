"""String manipulation utilities."""

import re


def snake_to_camel(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0].lower() + "".join(word.capitalize() for word in components[1:])


def get_natural_sort_key(text: str) -> list:
    """Convert text to a key for natural sorting.

    Natural sorting orders numbers within strings numerically rather than lexicographically.
    Example: ['Doc1', 'Doc2', 'Doc10'] instead of ['Doc1', 'Doc10', 'Doc2']

    Args:
        text: String to convert to natural sort key

    Returns:
        List of mixed integers and strings for sorting

    Example:
        >>> items = ['Doc10', 'Doc2', 'Doc1']
        >>> sorted(items, key=get_natural_sort_key)
        ['Doc1', 'Doc2', 'Doc10']
    """
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", text)]
