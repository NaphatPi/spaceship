"""Module testing query.py"""

import os
from contextlib import contextmanager
from typing import Literal
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from spaceship.exception import AmbiguousSourceTable
from spaceship.query import DuckDBQueryExecutor


@contextmanager
def does_not_raise():
    yield


@pytest.mark.parametrize(
    "query,mode,expectation",
    [
        (
            "SELECT * FROM do.my_bucket.my_dataset as d1 limit 10;",
            "delta_scan",
            "SELECT * FROM delta_scan('s3://my_bucket/my_dataset') AS d1 LIMIT 10",
        ),
        (
            "SELECT * FROM do.my_bucket.my_dataset as d1 limit 10;",
            "read_parquet",
            "SELECT * FROM read_parquet('s3://my_bucket/my_dataset', hive_partitioning = true) AS d1 LIMIT 10",
        ),
        (
            "SELECT * FROM lc.'some/path/'.my_dataset as d1 limit 10;",
            "delta_scan",
            "SELECT * FROM delta_scan('"
            f"file://{os.path.abspath(os.path.join('some/path/', 'my_dataset'))}"
            "') AS d1 LIMIT 10",
        ),
        (
            "SELECT * FROM lc.'some/path/my_dataset' as d1 limit 10;",
            "delta_scan",
            "SELECT * FROM delta_scan('"
            f"file://{os.path.abspath(os.path.join('some/path/', 'my_dataset'))}"
            "') AS d1 LIMIT 10",
        ),
    ],
)
def test_duckdb_valid_query_dataset_alias(query: str, mode: Literal["delta_scan", "read_parquet"], expectation: str):
    """Test duckdb query parser"""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        # Set up the mock to return the mock connection
        mock_connect.return_value = mock_conn
        # Set to None for now and we don't want to use this
        mock_conn.execute.return_value = None

        query_exe = DuckDBQueryExecutor(delta_read_mode=mode)
        assert expectation == query_exe.parse_query(query)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM DO.SOME_TABLE LIMIT 10;",
        "SELECT * FROM DO.SOME_DB.SOME_SCHEMA.SOME_TABLE LIMIT 10;",
        "SELECT * FROM LC.'/some/path'.schema.dataset_name LIMIT 10;",
    ],
)
def test_duckdb_invalid_query_dataset_alias(query: str):
    """Test duckdb query parser"""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        # Set up the mock to return the mock connection
        mock_connect.return_value = mock_conn
        # Set to None for now and we don't want to use this
        mock_conn.execute.return_value = None

        query_exe = DuckDBQueryExecutor()
        with pytest.raises(AmbiguousSourceTable):
            query_exe.parse_query(query)


def test_duckdb_invalid_query_mode():
    """Test duckdb invalid query parser mode"""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        # Set up the mock to return the mock connection
        mock_connect.return_value = mock_conn
        # Set to None for now and we don't want to use this
        mock_conn.execute.return_value = None

        mode = "invalid mode"
        query_exe = DuckDBQueryExecutor(delta_read_mode=mode)
        with pytest.raises(ValueError):
            query_exe.parse_query("SELECT * FROM do.my_db.my_table")
