"""Utilities module"""

import inspect
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from spaceship.metadata import DatasetMetadata


def get_valid_argument(param_dict: dict, func: Callable) -> dict:
    """This function extract params from param_dict and only include params
    that are acceptable by the func argument.
    """
    sig = inspect.signature(func)

    return {param: val for param, val in param_dict.items() if param in sig.parameters}
