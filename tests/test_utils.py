"""Module testing utils.py"""

from contextlib import contextmanager

import pandas as pd
import pyarrow as pa
import pytest

from spaceship.utils import validate_input_data_type


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "intput_data,expectation",
    [
        (pd.DataFrame(), does_not_raise()),
        (pa.Table.from_pandas(pd.DataFrame()), does_not_raise()),
        ([], pytest.raises(TypeError)),
    ],
)
def test_validate_input_data_type(intput_data, expectation):
    """Test different input datatype"""
    with expectation:
        assert validate_input_data_type(intput_data) is not None
