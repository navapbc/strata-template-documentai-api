import os
from functools import lru_cache

import boto3

from documentai_api.utils import env
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


class AWSClientFactory:
    _session: boto3.Session | None = None

    @classmethod
    def get_session(cls) -> boto3.Session:
        if cls._session is None:
            cls._session = boto3.Session()

        return cls._session

    @classmethod
    def _get_region(cls) -> str:
        return os.getenv("AWS_REGION", "us-east-1")

    @classmethod
    def _get_documentai_region(cls) -> str:
        return os.getenv(env.DOCUMENTAI_REGION, "us-east-1")

    @classmethod
    def _get_dynamodb_table(cls, table_name: str):
        return cls.get_dynamodb_resource().Table(table_name)

    @classmethod
    @lru_cache(maxsize=1)
    def get_s3_client(cls):
        return cls.get_session().client("s3", region_name=cls._get_documentai_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_dynamodb_resource(cls):
        return cls.get_session().resource("dynamodb", region_name=cls._get_documentai_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_bda_client(cls):
        """Get Bedrock Data Automation client for project/blueprint management."""
        return cls.get_session().client(
            "bedrock-data-automation", region_name=cls._get_documentai_region()
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_bda_runtime_client(cls):
        """Get Bedrock Data Automation Runtime client for job execution (invoke, get status)."""
        return cls.get_session().client(
            "bedrock-data-automation-runtime", region_name=cls._get_documentai_region()
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_ssm_client(cls):
        return cls.get_session().client("ssm", region_name=cls._get_documentai_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_sqs_client(cls):
        return cls.get_session().client("sqs", region_name=cls._get_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_athena_client(cls):
        return cls.get_session().client("athena", region_name=cls._get_region())

    @classmethod
    def get_ddb_table(cls, table_name: str):
        """Get DynamoDB table resource by name."""
        return cls._get_dynamodb_table(table_name)


__all__ = ["AWSClientFactory"]
