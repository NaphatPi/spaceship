"""Query related module"""

import logging
import os
from abc import (
    ABC,
    abstractmethod,
)
from typing import Literal

import duckdb
from sqlglot import (
    exp,
    parse,
)

from spaceship.exception import AmbiguousSourceTable

logger = logging.getLogger(__name__)


class QueryExecutor(ABC):
    """Base query executor class"""

    @abstractmethod
    def execute(self, query: str):
        """Execute query"""


class DuckDBQueryExecutor(QueryExecutor):
    """DuckDB query executor"""

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str | None = "nyc3",
        endpoint: str | None = "digitaloceanspaces.com",
        delta_read_mode: Literal["delta_scan", "read_parquet"] = "delta_scan",
    ):
        """Initialize a query executor instance"""
        self.delta_read_mode = delta_read_mode
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
                ENDPOINT '{endpoint}'
            );
        """
        )

    def execute(self, query: str, **kwargs):
        """Execute a duckdb sql query"""
        query = self.parse_query(query)

        # Register as a workaround to be able to query python object like pd.DataFrame
        if kwargs:
            for k, v in kwargs.items():
                self._con.register(k, v)

        return self._con.execute(query)

    def parse_query(self, query: str) -> str:
        """Parse query string replacing some keyword or matched patterns."""
        expressions = parse(query)
        for expression in expressions:
            if expression is not None:
                for table in expression.find_all(exp.Table):
                    self._parse_table(table)
        merged_query = ";\n".join(expr.sql() for expr in expressions if expr is not None)
        return merged_query

    def _parse_table(self, table: exp.Table) -> None:
        """Parse and transform table in the from clause to be in appropriate duck db format.
        The parsed value will be applied in-place.
        """
        statement = table.sql().casefold()
        logger.debug("Parsing from clause: %s", statement)
        if statement.startswith("do."):
            table_uri = DuckDBQueryExecutor._get_s3_dataset_uri(table)
        elif statement.startswith("lc."):
            table_uri = DuckDBQueryExecutor._get_local_dataset_uri(table)
        else:
            logger.debug("No tables needed to be parsed")
            return

        if self.delta_read_mode == "delta_scan":
            duckdb_source = f"delta_scan('{table_uri}')"
        elif self.delta_read_mode == "read_parquet":
            duckdb_source = f"read_parquet('{table_uri}', hive_partitioning = true)"
        else:
            raise ValueError(f"mode '{self.delta_read_mode}' is invalid")

        for component in table.parts:
            component.pop()

        table.set("this", duckdb_source)
        logger.debug("The from clause has been parsed to %s", duckdb_source)

    @staticmethod
    def _get_s3_dataset_uri(table: exp.Table) -> str:
        """Get s3-compatible dataset uri"""
        if table.catalog.casefold() != "do" or len(table.parts) > 3:
            raise AmbiguousSourceTable(
                f"Can't parse the source table {table.sql()}. "
                "If you meant to refer to a DigitalOcean Spaces dataset, "
                "use the pattern do.<you_bucket>.<your_dataset>"
            )
        _, bucket, dataset = table.parts
        return f"s3://{bucket.name}/{dataset.name}"

    @staticmethod
    def _get_local_dataset_uri(table: exp.Table) -> str:
        """Get local dataset uri"""
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
        return f"file://{path}"
