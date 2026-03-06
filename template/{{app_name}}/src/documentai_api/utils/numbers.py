"""Numeric utility functions."""


def normalize(value: float, min_val: float, max_val: float) -> float:
    """Scale a value to 0-1 range using min-max normalization.

    Args:
        value: Value to normalize
        min_val: Minimum value of the range
        max_val: Maximum value of the range

    Returns:
        Normalized value clamped to [0.0, 1.0]
    """
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))
