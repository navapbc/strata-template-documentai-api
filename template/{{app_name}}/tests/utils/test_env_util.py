"""Tests for utils/env.py."""

from documentai_api.utils import env


def test_environment_variable_names_are_defined():
    """Test that all environment variable names are defined as constants."""
    assert hasattr(env, "DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME")
    assert hasattr(env, "DOCUMENTAI_INPUT_LOCATION")
    assert hasattr(env, "DOCUMENTAI_OUTPUT_LOCATION")
    assert hasattr(env, "DOCUMENTAI_PROFILE_ARN")
    assert hasattr(env, "DOCUMENTAI_PROJECT_ARN")
    assert hasattr(env, "DOCUMENTAI_REGION")


def test_environment_variable_values():
    """Test that environment variable names have expected string values."""
    assert env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME == "DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME"
    assert env.DOCUMENTAI_INPUT_LOCATION == "DOCUMENTAI_INPUT_LOCATION"
    assert env.DOCUMENTAI_OUTPUT_LOCATION == "DOCUMENTAI_OUTPUT_LOCATION"
    assert env.DOCUMENTAI_PROFILE_ARN == "DOCUMENTAI_PROFILE_ARN"
    assert env.DOCUMENTAI_PROJECT_ARN == "DOCUMENTAI_PROJECT_ARN"
    assert env.DOCUMENTAI_REGION == "DOCUMENTAI_REGION"
