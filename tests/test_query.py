"""Module testing query.py"""

import os
from typing import Literal
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from spaceship.exception import AmbiguousSourceTable
from spaceship.query import (
    DuckDBQueryExecutor,
    FutureDatasetRef,
)


@pytest.mark.parametrize(
    "query,mode,expectation",
    [
        (
            "SELECT * FROM do.my_bucket.my_dataset_1 AS d1 LEFT JOIN do.my_bucket.my_dataset_2 AS d2 limit 10;",
            "pyarrow",
            (
                "SELECT * FROM f_my_dataset_1 AS d1 LEFT JOIN f_my_dataset_2 AS d2 LIMIT 10",
                [
                    FutureDatasetRef(name="my_dataset_1", alias="f_my_dataset_1", bucket="my_bucket"),
                    FutureDatasetRef(name="my_dataset_2", alias="f_my_dataset_2", bucket="my_bucket"),
                ],
            ),
        ),
        (
            "SELECT * FROM do.my_bucket.my_dataset_1 AS d1 LEFT JOIN do.my_bucket.my_dataset_2 AS d2 limit 10;",
            "duckdb",
            (
                "SELECT * FROM delta_scan('s3://my_bucket/my_dataset_1') AS d1 LEFT JOIN delta_scan('s3://my_bucket/my_dataset_2') AS d2 LIMIT 10",
                [],
            ),
        ),
        (
            "SELECT * FROM lc.'some/path/'.my_dataset as d1 limit 10;",
            "pyarrow",
            (
                "SELECT * FROM f_my_dataset AS d1 LIMIT 10",
                [FutureDatasetRef(name="my_dataset", alias="f_my_dataset", bucket=None)],
            ),
        ),
        (
            "SELECT * FROM lc.'some/path/my_dataset' as d1 limit 10;",
            "duckdb",
            (
                "SELECT * FROM delta_scan('"
                f"file://{os.path.abspath(os.path.join('some/path/', 'my_dataset'))}"
                "') AS d1 LIMIT 10",
                [],
            ),
        ),
    ],
)
def test_duckdb_valid_query_dataset_alias(
    query: str, mode: Literal["pyarrow", "duckdb"], expectation: tuple[str, list[FutureDatasetRef]]
):
    """Test duckdb query parser"""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        # Set up the mock to return the mock connection
        mock_connect.return_value = mock_conn
        # Set to None for now and we don't want to use this
        mock_conn.execute.return_value = None

        query_exe = DuckDBQueryExecutor()
        assert expectation == query_exe.parse_query(query, delta_read_mode=mode)


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
            query_exe.parse_query(query, delta_read_mode="duckdb")


def test_duckdb_invalid_query_mode():
    """Test duckdb invalid query parser mode"""
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        # Set up the mock to return the mock connection
        mock_connect.return_value = mock_conn
        # Set to None for now and we don't want to use this
        mock_conn.execute.return_value = None

        delta_read_mode = "invalid mode"
        query_exe = DuckDBQueryExecutor()
        with pytest.raises(ValueError):
            query_exe.parse_query("SELECT * FROM do.my_db.my_table", delta_read_mode=delta_read_mode)
