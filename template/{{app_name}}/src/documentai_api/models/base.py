from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class CamelCaseResponse(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel, populate_by_name=True, serialize_by_alias=True
    )


class BaseApiResponse(CamelCaseResponse):
    """Base class for all API response models."""

    pass
