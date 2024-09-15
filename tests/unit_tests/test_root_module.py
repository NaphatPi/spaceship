"""Root module sample tests."""

import time

import pytest

from spaceship.root_module import sum_2_numbers


@pytest.mark.slow
def test_slow_true():
    """Test `slow_add()`."""
    time.sleep(4)
    assert True


def test_sum_2_numbers():
    """Test sum_2_number function."""
    assert sum_2_numbers(1, 2) == 3


def test_session_id(session_id):
    """Test if session_id is not empty."""
    assert session_id != ""
