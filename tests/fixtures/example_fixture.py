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
    return Client()
