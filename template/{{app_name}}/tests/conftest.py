"""Shared test fixtures."""

import pytest

#############################################################################
# Autouse fixtures                                                          #
#                                                                           #
# Since these are defined here in the top-level conftest file, they apply   #
# globally to all tests.                                                    #
#############################################################################


@pytest.fixture(autouse=True, scope="session")
def reset_env():
    """Start each test suite run with a clean environment."""
    import os

    # save a copy of environment as it is at start of run
    env = dict(os.environ)

    os.environ.clear()

    # for native dependencies
    os.environ["PATH"] = env["PATH"]

    # for other fixtures that may want to reference real environment values for
    # their test settings
    return env


#######################
# API Server fixtures #
#######################


@pytest.fixture
def runtime_required_env(monkeypatch, s3_bucket, ddb_doc_metadata_table):
    """Required configuration to run the application in general."""
    from documentai_api.utils import env

    monkeypatch.setenv(env.DOCUMENTAI_INPUT_LOCATION, f"s3://{s3_bucket.name}/input")
    monkeypatch.setenv(env.DOCUMENTAI_OUTPUT_LOCATION, f"s3://{s3_bucket.name}/output")
    monkeypatch.setenv(env.BDA_PROJECT_ARN, "arn:aws:project")
    monkeypatch.setenv(env.BDA_PROFILE_ARN, "arn:aws:profile")
    monkeypatch.setenv(env.BDA_REGION, "us-east-1")


@pytest.fixture
def api_client(runtime_required_env):
    """Create test client."""
    from fastapi.testclient import TestClient

    from documentai_api.app import app

    return TestClient(app)


@pytest.fixture
def api_skeleton_key(monkeypatch):
    key = "foobar"
    monkeypatch.setenv("API_AUTH_INSECURE_SHARED_KEY", key)
    return key


#################################################################################
# Regular fixtures                                                              #
#                                                                               #
# Logical groups of fixtures should be grouped in tests/helpers/fixtures/ (and  #
# then imported at the bottom of this file) or live within conftest.py files in #
# various test directories. But general/misc. fixtures can live here.           #
#################################################################################


@pytest.fixture
def mock_grayscale_dependencies(mocker):
    mock_cv2_imdecode = mocker.patch("cv2.imdecode")
    mock_cv2_cvtcolor = mocker.patch("cv2.cvtColor")
    mock_pil_fromarray = mocker.patch("PIL.Image.fromarray")

    return mock_cv2_imdecode, mock_cv2_cvtcolor, mock_pil_fromarray


@pytest.fixture
def disable_tenacity_wait(mocker):
    """Make Tenacity wait for 0 seconds between retries.

    Generally
    """
    mocker.patch("tenacity.nap.time")


@pytest.fixture
def clear_env_vars():
    """Clear all environment variables.

    The test suite starts with an _almost_ clean environment by default, by if
    you want it cleaner you can use this. Pytest may internally still set some
    environment variables.
    """
    import os
    from unittest.mock import patch

    with patch.dict(os.environ, {}, clear=True):
        yield


######################
# Pytest setup stuff #
######################

pytest.register_assert_rewrite("tests.helpers")

pytest_plugins = (
    "tests.helpers.fixtures.aws",
    "tests.helpers.fixtures.db.ddb",
    "tests.helpers.fixtures.documents",
)
