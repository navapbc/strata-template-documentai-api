"""Tests for utils/env.py"""

from utils import env


def test_environment_variable_names_are_defined():
    """Test that all environment variable names are defined as constants."""
    assert hasattr(env, "DDE_DOCUMENT_METADATA_TABLE_NAME")
    assert hasattr(env, "DDE_INPUT_LOCATION")
    assert hasattr(env, "DDE_OUTPUT_LOCATION")
    assert hasattr(env, "DDE_PROFILE_ARN")
    assert hasattr(env, "DDE_PROJECT_ARN")
    assert hasattr(env, "DDE_REGION")


def test_environment_variable_values():
    """Test that environment variable names have expected string values."""
    assert env.DDE_DOCUMENT_METADATA_TABLE_NAME == "DDE_DOCUMENT_METADATA_TABLE_NAME"
    assert env.DDE_INPUT_LOCATION == "DDE_INPUT_LOCATION"
    assert env.DDE_OUTPUT_LOCATION == "DDE_OUTPUT_LOCATION"
    assert env.DDE_PROFILE_ARN == "DDE_PROFILE_ARN"
    assert env.DDE_PROJECT_ARN == "DDE_PROJECT_ARN"
    assert env.DDE_REGION == "DDE_REGION"
