"""Conftest for pytest."""

import sys
from pathlib import Path

THIS_DIR = Path(__file__).parent
ROOT_DIR = THIS_DIR.parent

sys.path.insert(0, str(ROOT_DIR))

pytest_plugins = ["tests.fixtures.fixture"]
