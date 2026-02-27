from io import BytesIO

from fastapi import UploadFile


def create_upload_file_from_bytes(file_bytes: bytes, filename: str) -> UploadFile:
    """Create an UploadFile object from bytes."""
    return UploadFile(filename=filename, file=BytesIO(file_bytes))
