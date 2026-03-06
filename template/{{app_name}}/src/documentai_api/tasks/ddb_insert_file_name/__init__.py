"""BDA output processor task."""

from documentai_api.tasks.ddb_insert_file_name.main import (
    convert_s3_object_to_grayscale,
    convert_to_grayscale,
    is_file_too_large_for_bda,
    main,
)

__all__ = [
    "convert_s3_object_to_grayscale",
    "convert_to_grayscale",
    "is_file_too_large_for_bda",
    "main",
]
