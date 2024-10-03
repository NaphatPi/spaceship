"""Example fixtures."""

from uuid import uuid4

import pytest

from spaceship.client import Client


@pytest.fixture(scope="session")
def session_id() -> str:
    """Test session id."""
    test_session_id = str(uuid4())[:6]
    return test_session_id


@pytest.fixture(scope="session")
def client() -> Client:
    """Local client fixture"""
    return Client(
        access_key="test_access_key", secret_key="test_secret_key", region="test_region", endpoint="test_endpoint"
    )
