from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class PydanticBaseEnvConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class AWSEnvConfig(PydanticBaseEnvConfig):
    # aws related config
    bda_project_arn: str
    bda_profile_arn: str
    bda_region: str = "us-east-1"
    documentai_document_metadata_table_name: str
    documentai_document_metadata_job_id_index_name: str
    documentai_input_location: str
    documentai_output_location: str
    max_bda_invoke_retry_attempts: int = 3


class AppEnvConfig(PydanticBaseEnvConfig):
    # general config
    api_auth_insecure_shared_key: str = ""
    image_tag: str = ""
    environment: str = "local"


@lru_cache
def get_aws_config() -> AWSEnvConfig:
    return AWSEnvConfig()  # type: ignore[call-arg]


@lru_cache
def get_app_env_config() -> AppEnvConfig:
    return AppEnvConfig()
