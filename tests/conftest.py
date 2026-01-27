"""
Shared pytest fixtures for BrickKit tests.

Provides environment management, test resource naming, and common utilities.
"""

import os
import uuid
from datetime import datetime
from typing import Generator

import pytest


def generate_test_prefix() -> str:
    """
    Generate a unique prefix for test resources.

    Format: brickkit_test_{timestamp}_{short_uuid}
    Example: brickkit_test_20240127_143052_abc123
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:6]
    return f"brickkit_test_{timestamp}_{short_uuid}"


@pytest.fixture(scope="session")
def test_prefix() -> str:
    """
    Session-scoped unique prefix for test resources.

    Use this to create resource names that won't collide with
    existing resources or parallel test runs.
    """
    return generate_test_prefix()


@pytest.fixture
def dev_environment() -> Generator[None, None, None]:
    """
    Fixture that sets DATABRICKS_ENV to DEV for the test duration.

    Restores the original value after the test completes.
    """
    original = os.environ.get("DATABRICKS_ENV")
    os.environ["DATABRICKS_ENV"] = "DEV"
    yield
    if original is not None:
        os.environ["DATABRICKS_ENV"] = original
    elif "DATABRICKS_ENV" in os.environ:
        del os.environ["DATABRICKS_ENV"]


@pytest.fixture
def acc_environment() -> Generator[None, None, None]:
    """
    Fixture that sets DATABRICKS_ENV to ACC for the test duration.

    Restores the original value after the test completes.
    """
    original = os.environ.get("DATABRICKS_ENV")
    os.environ["DATABRICKS_ENV"] = "ACC"
    yield
    if original is not None:
        os.environ["DATABRICKS_ENV"] = original
    elif "DATABRICKS_ENV" in os.environ:
        del os.environ["DATABRICKS_ENV"]


@pytest.fixture
def prd_environment() -> Generator[None, None, None]:
    """
    Fixture that sets DATABRICKS_ENV to PRD for the test duration.

    Restores the original value after the test completes.
    """
    original = os.environ.get("DATABRICKS_ENV")
    os.environ["DATABRICKS_ENV"] = "PRD"
    yield
    if original is not None:
        os.environ["DATABRICKS_ENV"] = original
    elif "DATABRICKS_ENV" in os.environ:
        del os.environ["DATABRICKS_ENV"]


@pytest.fixture(params=["DEV", "ACC", "PRD"])
def all_environments(request: pytest.FixtureRequest) -> Generator[str, None, None]:
    """
    Parametrized fixture that runs tests in all environments.

    Use this to test environment-aware naming across all environments.
    """
    env = request.param
    original = os.environ.get("DATABRICKS_ENV")
    os.environ["DATABRICKS_ENV"] = env
    yield env
    if original is not None:
        os.environ["DATABRICKS_ENV"] = original
    elif "DATABRICKS_ENV" in os.environ:
        del os.environ["DATABRICKS_ENV"]


@pytest.fixture(autouse=True)
def reset_environment() -> Generator[None, None, None]:
    """
    Autouse fixture that ensures tests start with DEV environment.

    This prevents environment bleed between tests.
    """
    original = os.environ.get("DATABRICKS_ENV")
    os.environ["DATABRICKS_ENV"] = "DEV"
    yield
    if original is not None:
        os.environ["DATABRICKS_ENV"] = original
    elif "DATABRICKS_ENV" in os.environ:
        del os.environ["DATABRICKS_ENV"]
