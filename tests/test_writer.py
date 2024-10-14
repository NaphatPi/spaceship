"""Tests for writer module"""

from pathlib import Path
from typing import Generator
from unittest.mock import (
    Mock,
    patch,
)

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import (
    DeltaTable,
    write_deltalake,
)

from spaceship.metadata import DatasetMetadata
from spaceship.writer import (
    _append_multi_tables,
    _restore_deltatable,
    _table_generator,
    write_data,
)


@pytest.mark.parametrize(
    "data",
    [
        "pandas_df",
        "pyarrow_table",
        "parquet_file",
        "csv_file",
        "pyarrow_dataset",
    ],
)
def test_append(data, request, pandas_df: pd.DataFrame, deltatable: DeltaTable, dataset_metadata: DatasetMetadata):
    """Test appending"""
    data = request.getfixturevalue(data)
    write_data(data, table_uri=deltatable.table_uri, metadata=dataset_metadata, mode="append")
    df = DeltaTable(deltatable.table_uri).to_pandas().sort_values(by="id", ascending=True).reset_index(drop=True)
    assert df.shape[0] == pandas_df.shape[0]
    assert df.equals(pandas_df)


def test_append_multipart(pandas_df: pd.DataFrame, deltatable: DeltaTable, dataset_metadata: DatasetMetadata):
    """Test appending multipart"""
    df = pd.concat([pandas_df for _ in range(5000)]).sort_values(by="id", ascending=True).reset_index(drop=True)

    with (
        patch("spaceship.writer.psutil.virtual_memory") as mock_virtual_mem_call,
        patch("spaceship.writer._append_multi_tables", side_effect=_append_multi_tables) as mock_multi_call,
    ):
        mock_virtual_mem = Mock()
        mock_virtual_mem.available = 3_000_000

        mock_virtual_mem_call.return_value = mock_virtual_mem

        write_data(df, table_uri=deltatable.table_uri, metadata=dataset_metadata, mode="append", max_chunksize=10000)
        stored_df = (
            DeltaTable(deltatable.table_uri).to_pandas().sort_values(by="id", ascending=True).reset_index(drop=True)
        )
        mock_multi_call.assert_called_once()
        assert df.shape[0] == stored_df.shape[0]
        assert df.equals(stored_df)


def test_failed_append_multipart(pandas_df: pd.DataFrame, deltatable: DeltaTable, dataset_metadata: DatasetMetadata):
    """Test failed appending multipart"""
    df = pd.concat([pandas_df for _ in range(5000)]).sort_values(by="id", ascending=True).reset_index(drop=True)

    num_failed_table_generator_called = 0
    FAIL_AFTER = 4  # number of iteration before faling

    class DummyError(Exception):
        """dummy exception class"""

    def failed_table_generator(*args, **kwargs) -> Generator[pa.Table, None, None]:
        """Mock table generator function to fail after the 4th iteration.
        This function will allow the first call to return the actual table generator.
        """
        nonlocal num_failed_table_generator_called

        # This is to allow the first call to work normally
        if num_failed_table_generator_called == 0:
            num_failed_table_generator_called += 1
            yield from _table_generator(*args, **kwargs)
        else:
            for _ in range(FAIL_AFTER):
                yield pa.Table.from_pandas(pandas_df), False

            raise DummyError("Mock some error thrown")

    with (
        patch("spaceship.writer._table_generator") as mock_table_generator_call,
        patch("spaceship.writer.psutil.virtual_memory") as mock_virtual_mem_call,
        patch("spaceship.writer._restore_deltatable", side_effect=_restore_deltatable) as mock_restore_call,
        patch("spaceship.writer._append_multi_tables", side_effect=_append_multi_tables) as mock_multi_call,
    ):
        mock_virtual_mem = Mock()
        mock_virtual_mem.available = 3_000_000
        mock_virtual_mem_call.return_value = mock_virtual_mem

        mock_table_generator_call.side_effect = failed_table_generator

        # add dummy data
        write_deltalake(deltatable, pandas_df, mode="append")

        start_version = deltatable.version()

        with pytest.raises(DummyError):
            write_data(
                df,
                table_uri=deltatable.table_uri,
                metadata=dataset_metadata,
                mode="append",
                max_chunksize=10000,
            )

        now_table = DeltaTable(deltatable.table_uri)
        stored_df = now_table.to_pandas().sort_values(by="id", ascending=True).reset_index(drop=True)

        mock_multi_call.assert_called_once()
        mock_restore_call.assert_called_once()
        assert now_table.version() == start_version + FAIL_AFTER + 1
        assert pandas_df.shape[0] == stored_df.shape[0]
        assert pandas_df.equals(stored_df)


def test_write_invalid_schema(
    invalid_schema_pandas_df: pd.DataFrame, deltatable: DeltaTable, dataset_metadata: DatasetMetadata
):
    """Test writing with invalid schema"""
    with pytest.raises(ValueError):
        write_data(invalid_schema_pandas_df, table_uri=deltatable.table_uri, metadata=dataset_metadata, mode="append")


def test_write_invalid_file_extension(tmp_path: Path, deltatable: DeltaTable, dataset_metadata: DatasetMetadata):
    """Test write with invalid file extension"""
    path = tmp_path / "test_file.yaml"
    path.write_text("id: 1")
    with pytest.raises(ValueError):
        write_data("", table_uri=deltatable.table_uri, metadata=dataset_metadata, mode="append")


def test_write_unsupported_datatype(deltatable: DeltaTable, dataset_metadata: DatasetMetadata):
    """Test write with unsupported_datatype"""
    with pytest.raises(TypeError):
        write_data(
            {"name": ["mike", "jeff"], "age": [23, 30]},
            table_uri=deltatable.table_uri,
            metadata=dataset_metadata,
            mode="append",
        )
