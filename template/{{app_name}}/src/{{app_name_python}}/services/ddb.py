"""DynamoDB service methods."""

from utils.aws_client_factory import AWSClientFactory


def get_item(table_name: str, key: dict, consistent_read: bool = True) -> dict:
    """Get item from DynamoDB table."""
    ddb_table = AWSClientFactory.get_ddb_table(table_name)
    response = ddb_table.get_item(Key=key, ConsistentRead=consistent_read)
    return response.get("Item")


def put_item(table_name: str, item: dict) -> None:
    """Put item to DynamoDB table."""
    ddb_table = AWSClientFactory.get_ddb_table(table_name)
    ddb_table.put_item(Item=item)


def update_item(
    table_name: str, key: dict, update_expression: str, expression_values: dict
) -> None:
    """Update item in DynamoDB table."""
    ddb_table = AWSClientFactory.get_ddb_table(table_name)
    ddb_table.update_item(
        Key=key, UpdateExpression=update_expression, ExpressionAttributeValues=expression_values
    )


def query_by_key(table_name: str, index_name: str, key_name: str, key_value: str) -> list:
    """Query DynamoDB table by key using GSI."""
    import boto3

    ddb_table = AWSClientFactory.get_ddb_table(table_name)
    response = ddb_table.query(
        IndexName=index_name,
        KeyConditionExpression=boto3.dynamodb.conditions.Key(key_name).eq(key_value),
    )
    return response.get("Items", [])
