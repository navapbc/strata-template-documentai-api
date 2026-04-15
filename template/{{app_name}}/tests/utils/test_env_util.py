"""Tests for utils/env.py."""

from documentai_api.utils.env import AWSEnvConfig


def test_aws_env_config_has_required_fields():
    fields = AWSEnvConfig.model_fields
    assert "bda_project_arn" in fields
    assert "bda_profile_arn" in fields
    assert "documentai_input_location" in fields
    assert "documentai_output_location" in fields
    assert "documentai_document_metadata_table_name" in fields
    assert "documentai_document_metadata_job_id_index_name" in fields


def test_aws_env_config_defaults():
    fields = AWSEnvConfig.model_fields
    assert fields["bda_region"].default == "us-east-1"
    assert fields["environment"].default == "local"
    assert fields["max_bda_invoke_retry_attempts"].default == 3
    assert fields["api_auth_insecure_shared_key"].default == ""
    assert fields["image_tag"].default == ""
