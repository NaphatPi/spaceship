"""Query related module"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb
from sqlglot import (
    exp,
    parse,
)

from spaceship.exception import AmbiguousSourceTable

logger = logging.getLogger(__name__)


@dataclass
class FutureDatasetRef:
    """Reference of arrow dataset with the dataset name and alias used in the query
    Args:
        name (str): dataset name or path to dataset directory
        alias (str): alias used in the sql FROM clause
        bucket_name (str | None): bucket name of the dataset if it's in an object store.
    """

    name: str
    alias: str
    bucket: str | None


class DuckDBQueryExecutor:
    """DuckDB query executor"""

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str | None = "nyc3",
        endpoint: str | None = "digitaloceanspaces.com",
    ):
        """Initialize a query executor instance"""
        self._con = duckdb.connect()
        self._con.execute("INSTALL 'delta'")
        self._con.execute("LOAD 'delta'")
        self._con.execute(
            f"""
            CREATE SECRET dosecret (
                TYPE S3,
                KEY_ID '{access_key or os.getenv("ACCESS_KEY")}',
                SECRET '{secret_key or os.getenv("SECRET_KEY")}',
                REGION '{region}',
                ENDPOINT '{region}.{endpoint}'
            );
        """
        )

    def execute(self, query: str, **kwargs):
        """Execute a duckdb sql query"""
        # Register as a workaround to be able to query python object like pd.DataFrame
        if kwargs:
            for k, v in kwargs.items():
                self._con.register(k, v)

        return self._con.execute(query)

    def parse_query(
        self, query: str, delta_read_mode: Literal["pyarrow", "duckdb"]
    ) -> tuple[str, list[FutureDatasetRef]]:
        """Parse query string replacing some keyword or matched patterns.

        Args:
            query (str): query string to be parsed
            delta_read_mode (Literal['pyarrow', 'duckdb']): Mode to read deltatable. If pyarrow, pyarrow dataset will be
                required to provided along side the parsed query when calling the execute moethod.

        Return:
            A tuple containing 2 things:
                1. Parsed query string
                2. List containing ArrowDatasetRef(s) which are pyarrow datasets required to be provided
                    along with the query when calling execute.
        """
        dataset_refs: list[FutureDatasetRef] = []
        expressions = parse(query)
        for expression in expressions:
            if expression is not None:
                for table in expression.find_all(exp.Table):
                    dataset_ref = self._parse_table(table, delta_read_mode)
                    if dataset_ref is not None:
                        dataset_refs.append(dataset_ref)
        merged_query = ";\n".join(expr.sql() for expr in expressions if expr is not None)
        return merged_query, dataset_refs

    def _parse_table(self, table: exp.Table, delta_read_mode: Literal["pyarrow", "duckdb"]) -> FutureDatasetRef | None:
        """Parse and transform table in the from clause to be in appropriate duckdb format.
        Note that the table will be edited in-place.

        Args:
            table: sqlglot exp.Table
            delta_read_mode: String specifying how to parse deltatable dataset reference

        Return:
            A list of ArrowDatasetRef(s)
        """
        logger.debug("Parsing from clause: %s", table.sql())
        statement = table.sql().casefold()

        if statement.startswith("do."):
            dataset, bucket = _parse_s3_dataset(table)
        elif statement.startswith("lc."):
            dataset, bucket = _parse_local_dataset(table), None
        else:
            logger.debug("No table needed to be parsed")
            return None

        # Remove original table names
        for component in table.parts:
            component.pop()

        deltatable = Path(dataset).stem
        if delta_read_mode == "pyarrow":
            duckdb_source = f"f_{deltatable}"
            dataset_ref = FutureDatasetRef(name=deltatable, alias=duckdb_source, bucket=bucket)
        elif delta_read_mode == "duckdb":
            table_uri = f"s3://{bucket}/{deltatable}" if bucket else f"file://{dataset}"
            dataset_ref = None
            duckdb_source = f"delta_scan('{table_uri}')"
        else:
            raise ValueError(f"delta_read_mode {delta_read_mode} is invalid")

        table.set("this", duckdb_source)
        logger.debug("The from clause has been parsed to %s", table.sql())

        return dataset_ref


def _parse_s3_dataset(table: exp.Table) -> tuple[str, str]:
    """Parse s3 dataset reference pattern"""
    if table.catalog.casefold() != "do" or len(table.parts) > 3:
        raise AmbiguousSourceTable(
            f"Can't parse the source table {table.sql()}. "
            "If you meant to refer to a DigitalOcean Spaces dataset, "
            "use the pattern do.<you_bucket>.<your_dataset>"
        )
    _, bucket, dataset = table.parts
    return dataset.name, bucket.name


def _parse_local_dataset(table: exp.Table) -> str:
    """Parse local dataset pattern. Returns path to dataset directory"""
    if table.catalog.casefold() == "lc" and len(table.parts) == 3:
        _, path, dataset = table.parts
        path = os.path.abspath(os.path.join(path.name, dataset.name))  # type:ignore
    elif table.db.casefold() == "lc":
        path = os.path.abspath(table.parts[1].name)  # type:ignore
    else:
        raise AmbiguousSourceTable(
            f"Can't parse the source table {table.sql()}. "
            "If you meant to refer to a local dataset, "
            "use lc.<path_to_dataset_dir_only>.<your_dataset> or "
            "lc.<path_to_dataset_including_dataset_name> pattern. You may need to "
            "wrap the path with double or single quote if it contains special characters or spaces."
        )
    return str(path)
