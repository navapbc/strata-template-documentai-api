from documentai_api.utils.aws_client_factory import AWSClientFactory
from documentai_api.utils.logger import get_logger

logger = get_logger(__name__)


def get_parameter(name: str) -> str:
    """Get SSM parameter value."""
    ssm = AWSClientFactory.get_ssm_client()
    response = ssm.get_parameter(Name=name)
    return response["Parameter"]["Value"]
