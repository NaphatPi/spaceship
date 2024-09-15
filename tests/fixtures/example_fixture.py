"""Example fixtures."""

from uuid import uuid4

import pytest


@pytest.fixture(scope="session")
def session_id() -> str:
    """Test session id."""
    test_session_id = str(uuid4())[:6]
    return test_session_id
