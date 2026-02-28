import io
import os
import zipfile

from fastapi import HTTPException, UploadFile

from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


async def extract_files_from_zip(zip_file: UploadFile) -> list[UploadFile]:
    """Extract files from zip archive and return as UploadFile list.

    Handles nested directories - extracts all files using basename only.
    """
    try:
        zip_content = await zip_file.read()
        files = []

        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # zf.infolist() returns all directories and files recursively
            # filter out directories and process files only
            for file_info in zf.infolist():
                if file_info.is_dir():
                    continue

                # extract file content
                file_content = zf.read(file_info.filename)

                # create UploadFile from extracted content
                upload_file = UploadFile(
                    filename=os.path.basename(file_info.filename), file=io.BytesIO(file_content)
                )
                files.append(upload_file)

        logger.info(f"Extracted {len(files)} files from {zip_file.filename}")
        return files

    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid zip file") from None
    except Exception as e:
        logger.error(f"Error extracting zip: {e}")
        raise HTTPException(status_code=500, detail="Failed to extract zip file") from e
