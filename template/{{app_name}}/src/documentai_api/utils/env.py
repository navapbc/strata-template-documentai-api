import os

from pydantic_settings import BaseSettings, SettingsConfigDict

# environment variable names
API_AUTH_INSECURE_SHARED_KEY = "API_AUTH_INSECURE_SHARED_KEY"
BDA_PROFILE_ARN = "BDA_PROFILE_ARN"
BDA_PROJECT_ARN = "BDA_PROJECT_ARN"
BDA_REGION = "BDA_REGION"
DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME = "DOCUMENTAI_DOCUMENT_METADATA_JOB_ID_INDEX_NAME"
DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME = "DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME"
DOCUMENTAI_INPUT_LOCATION = "DOCUMENTAI_INPUT_LOCATION"
DOCUMENTAI_OUTPUT_LOCATION = "DOCUMENTAI_OUTPUT_LOCATION"
MAX_BDA_INVOKE_RETRY_ATTEMPTS = "MAX_BDA_INVOKE_RETRY_ATTEMPTS"


class PydanticBaseEnvConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise ValueError(f"{name} environment variable not set")

    return value
