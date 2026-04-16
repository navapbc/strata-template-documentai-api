"""BDA schema management."""

import json
import os
from typing import Any, cast

from documentai_api.config.constants import (
    CACHE_BLUEPRINT_SCHEMAS_TTL_MINUTES,
    CACHE_KEY_BLUEPRINT_SCHEMAS,
)
from documentai_api.logging import get_logger
from documentai_api.services.bda import get_blueprint, get_data_automation_project
from documentai_api.utils import env
from documentai_api.utils.cache import get_cache

logger = get_logger(__name__)


def _fetch_schemas_from_bda() -> dict[str, Any]:
    """Fetch schemas from BDA."""
    msg = "Fetching schemas from BDA"
    logger.info(msg)

    project_arn = os.getenv(env.BDA_PROJECT_ARN)
    if not project_arn:
        msg = f"{env.BDA_PROJECT_ARN} not set"
        logger.error(msg)
        return {}

    try:
        # get project with blueprints
        project_response = get_data_automation_project(project_arn)
        blueprints = (
            project_response.get("project", {})
            .get("customOutputConfiguration", {})
            .get("blueprints", [])
        )

        schemas = {}

        for blueprint_config in blueprints:
            blueprint_arn = blueprint_config.get("blueprintArn")
            if not blueprint_arn:
                continue

            blueprint_response = get_blueprint(blueprint_arn)
            blueprint = blueprint_response.get("blueprint", {})
            schema_str = blueprint.get("schema", "{}")
            schema = json.loads(schema_str)
            document_type = schema.get("class", blueprint.get("blueprintName", "Unknown"))

            fields = _extract_fields(schema)

            schemas[document_type] = {"documentType": document_type, "fields": fields}

        msg = f"Fetched {len(schemas)} schemas from BDA"
        logger.info(msg)
        return schemas

    except Exception as e:
        msg = f"Failed to fetch schemas from BDA: {e}"
        logger.error(msg)
        return {}


def _extract_fields(schema: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract field list from schema."""
    fields = []
    properties = schema.get("properties", {})
    definitions = schema.get("definitions", {})

    for field_name, field_spec in properties.items():
        if "$ref" in field_spec:
            ref_name = field_spec["$ref"].split("/")[-1]
            nested_def = definitions.get(ref_name, {})
            nested_props = nested_def.get("properties", {})

            for nested_field, nested_spec in nested_props.items():
                full_name = f"{field_name}.{nested_field}"
                fields.append(
                    {
                        "name": full_name,
                        "type": nested_spec.get("type", "string"),
                        "description": nested_spec.get("instruction", ""),
                    }
                )
        elif field_spec.get("type") == "array":
            items = field_spec.get("items", {})
            if "$ref" in items:
                ref_name = items["$ref"].split("/")[-1]
                nested_def = definitions.get(ref_name, {})
                nested_props = nested_def.get("properties", {})

                for nested_field, nested_spec in nested_props.items():
                    full_name = f"{field_name}.{nested_field}"
                    fields.append(
                        {
                            "name": full_name,
                            "type": nested_spec.get("type", "string"),
                            "description": nested_spec.get("instruction", ""),
                        }
                    )
            else:
                fields.append(
                    {
                        "name": field_name,
                        "type": "array",
                        "description": field_spec.get("instruction", ""),
                    }
                )
        else:
            fields.append(
                {
                    "name": field_name,
                    "type": field_spec.get("type", "string"),
                    "description": field_spec.get("instruction", ""),
                }
            )

    return fields


def get_all_schemas() -> dict[str, Any]:
    """Get all document schemas."""
    cache = get_cache()

    # try cache first
    schemas = cache.get(CACHE_KEY_BLUEPRINT_SCHEMAS)
    if schemas is not None:
        return cast(dict[str, Any], schemas)

    # fetch from BDA and cache
    schemas = _fetch_schemas_from_bda()
    cache.add(CACHE_KEY_BLUEPRINT_SCHEMAS, schemas, ttl_minutes=CACHE_BLUEPRINT_SCHEMAS_TTL_MINUTES)

    return schemas


def get_document_schema(document_type: str) -> dict[str, Any] | None:
    """Get schema for specific document type."""
    schemas = get_all_schemas()
    return schemas.get(document_type)


def invalidate_schema_cache() -> None:
    """Force refresh of schema cache."""
    cache = get_cache()
    cache.invalidate(CACHE_KEY_BLUEPRINT_SCHEMAS)
