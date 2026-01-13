import os
from functools import lru_cache
from typing import Optional

import boto3


class AWSClientFactory:
    _session: Optional[boto3.Session] = None

    @classmethod
    def get_session(cls) -> boto3.Session:
        if cls._session is None:
            profile_name = os.getenv("AWS_PROFILE")
            cls._session = (
                boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
            )

        return cls._session

    @classmethod
    def _get_region(cls) -> str:
        return os.getenv("STACK_REGION", "us-east-1")

    @classmethod
    def _get_dynamodb_table(cls, table_name: str):
        return cls.get_dynamodb_resource().Table(table_name)

    @classmethod
    @lru_cache(maxsize=1)
    def get_s3_client(cls):
        return cls.get_session().client("s3", region_name=cls._get_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_dynamodb_resource(cls):
        return cls.get_session().resource("dynamodb", region_name=cls._get_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_bda_runtime_client(cls):
        return cls.get_session().client(
            "bedrock-data-automation-runtime", region_name=cls._get_region()
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_ssm_client(cls):
        return cls.get_session().client("ssm", region_name=cls._get_region())

    @classmethod
    def get_ddb_metadata_table(cls):
        table_name = os.getenv("DDE_DOCUMENT_METADATA_TABLE_NAME")

        if not table_name:
            raise ValueError("DDE_DOCUMENT_METADATA_TABLE_NAME environment variable not set")

        return cls._get_dynamodb_table(table_name)


__all__ = ["AWSClientFactory"]
