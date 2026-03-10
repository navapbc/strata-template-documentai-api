"""SSM Service methods."""

from documentai_api.utils.aws_client_factory import AWSClientFactory


def get_parameter(name: str, with_decryption: bool = True) -> str:
    """Get parameter from SSM Parameter Store."""
    ssm_client = AWSClientFactory.get_ssm_client()
    response = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
    return response["Parameter"]["Value"]
