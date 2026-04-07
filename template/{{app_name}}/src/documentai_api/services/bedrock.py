import json

from documentai_api.utils.aws_client_factory import AWSClientFactory


def invoke_model(model_id: str, body: dict) -> dict:
    client = AWSClientFactory.get_bedrock_runtime_client()
    response = client.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body),
    )
    return json.loads(response["body"].read())
