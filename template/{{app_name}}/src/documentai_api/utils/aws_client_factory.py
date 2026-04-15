from __future__ import annotations

import os
from functools import lru_cache
from typing import TYPE_CHECKING

import boto3

from documentai_api.logging import get_logger
from documentai_api.utils import env

if TYPE_CHECKING:
    from mypy_boto3_bedrock_data_automation.client import DataAutomationforBedrockClient
    from mypy_boto3_bedrock_data_automation_runtime.client import (
        RuntimeforBedrockDataAutomationClient,
    )
    from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_ssm.client import SSMClient


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
    def _get_bda_region(cls) -> str:
        return os.getenv(env.BDA_REGION, "us-east-1")

    @classmethod
    def _get_dynamodb_table(cls, table_name: str) -> Table:
        return cls.get_dynamodb_resource().Table(table_name)

    @classmethod
    @lru_cache(maxsize=1)
    def get_s3_client(cls) -> S3Client:
        return cls.get_session().client("s3", region_name=cls._get_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_dynamodb_resource(cls) -> DynamoDBServiceResource:
        return cls.get_session().resource("dynamodb", region_name=cls._get_region())

    @classmethod
    @lru_cache(maxsize=1)
    def get_bda_client(cls) -> DataAutomationforBedrockClient:
        """Get Bedrock Data Automation client for project/blueprint management."""
        return cls.get_session().client(
            "bedrock-data-automation", region_name=cls._get_bda_region()
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_bda_runtime_client(cls) -> RuntimeforBedrockDataAutomationClient:
        """Get Bedrock Data Automation Runtime client for job execution (invoke, get status)."""
        return cls.get_session().client(
            "bedrock-data-automation-runtime", region_name=cls._get_bda_region()
        )

    @classmethod
    @lru_cache(maxsize=1)
    def get_ssm_client(cls) -> SSMClient:
        return cls.get_session().client("ssm", region_name=cls._get_region())

    @classmethod
    def get_ddb_table(cls, table_name: str) -> Table:
        """Get DynamoDB table resource by name."""
        return cls._get_dynamodb_table(table_name)


__all__ = ["AWSClientFactory"]
