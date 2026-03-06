"""Tests for utils/env.py."""

from documentai_api.utils import env


def test_environment_variable_names_are_defined():
    """Test that all environment variable names are defined as constants."""
    assert hasattr(env, "DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME")
    assert hasattr(env, "DOCUMENTAI_INPUT_LOCATION")
    assert hasattr(env, "DOCUMENTAI_OUTPUT_LOCATION")
    assert hasattr(env, "BDA_PROFILE_ARN")
    assert hasattr(env, "BDA_PROJECT_ARN")
    assert hasattr(env, "BDA_REGION")


def test_environment_variable_values():
    """Test that environment variable names have expected string values."""
    assert env.DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME == "DOCUMENTAI_DOCUMENT_METADATA_TABLE_NAME"
    assert env.DOCUMENTAI_INPUT_LOCATION == "DOCUMENTAI_INPUT_LOCATION"
    assert env.DOCUMENTAI_OUTPUT_LOCATION == "DOCUMENTAI_OUTPUT_LOCATION"
    assert env.BDA_PROFILE_ARN == "BDA_PROFILE_ARN"
    assert env.BDA_PROJECT_ARN == "BDA_PROJECT_ARN"
    assert env.BDA_REGION == "BDA_REGION"
