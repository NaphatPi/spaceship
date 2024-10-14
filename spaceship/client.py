"""Spaceship client module"""

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs
from deltalake import DeltaTable
from pyarrow import Schema

from spaceship.exception import (
    DatasetAlreadyExists,
    DatasetNameNotAllowed,
    DatasetNotFound,
)
from spaceship.metadata import DatasetMetadata
from spaceship.query import DuckDBQueryExecutor
from spaceship.writer import write_data

# This is to allow working with S3 storage back end without locking mechanism
if not os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME"):
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
# This is a worksround for DeltaTable.is_deltatable check to work without S3 IMDS warning delay
if not os.getenv("AWS_EC2_METADATA_DISABLED"):
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class Client:
    """Spaceship Client class"""

    def __init__(
        self,
        *,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str | None = "nyc3",
        endpoint: str | None = "digitaloceanspaces.com",
    ):
        self.access_key = access_key or os.getenv("ACCESS_KEY")
        self.secret_key = secret_key or os.getenv("SECRET_KEY")
        self.region = region
        self.endpoint = endpoint
        self.query_executor: None | DuckDBQueryExecutor = None

    def create_dataset(
        self,
        name_or_path: str,
        schema: Schema,
        description: str,
        constraints: dict[str, str] | None = None,
        bucket: str | None = None,
        partition_columns: list[str] | str | None = None,
    ) -> DeltaTable:
        """
        Create a dataset with the given schema and constraints.

        Args:
            name_or_path (str): The name or path of the dataset.
            schema (Schema): The schema of the dataset.
            description (str): A description of the dataset.
            constraints (dict[str, str] | None, optional): Constraints to apply to the dataset. Defaults to None.
            bucket (str | None, optional): The name of the S3 bucket. Defaults to None.
            partition_columns (list[str] | str | None, optional): Columns to partition the dataset by. Defaults to None.
                If partition_columns is not provided, the default, load_parition_date, will be used, which is the date
                the data is loaded into the dataset.

        Raises:
            DatasetAlreadyExists: If the dataset or directory already exists.
            DatasetNameNotAllowed: If the table name is not valid.

        Returns:
            DeltaTable: The created DeltaTable object.
        """
        if self._path_exists(name_or_path, bucket):
            raise DatasetAlreadyExists(f"Dataset or directory {name_or_path} already exists")

        storage_options = self._get_storage_option() if bucket else None
        table_name = _validate_table_name(Path(name_or_path).stem)
        delta_table_path = self._get_table_uri(name_or_path, bucket)

        if not partition_columns:
            partition_columns = ["load_partition_date"]
            schema = schema.append(pa.field("load_partition_date", pa.date32()))

        try:
            dt = DeltaTable.create(
                name=table_name,
                table_uri=delta_table_path,
                schema=schema,
                description=description,
                mode="error",
                partition_by=partition_columns,
                storage_options=storage_options,
            )
            if constraints:
                dt.alter.add_constraint(constraints)
        except BaseException:
            logger.exception("Failed to provision dataset %s. The following error occured", name_or_path)
            logger.info("Deleting directory that might have been partially created")
            self._delete_dataset_if_exists(name_or_path, bucket)
            raise

        return dt

    def _get_storage_option(self) -> dict[str, str]:
        """Get storage option for connecting to S3-compatible storage."""
        return {
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
            "endpoint": f"https://{self.region}.{self.endpoint}",
        }

    def _get_arrow_fs(self, mode: Literal["local", "s3"]):
        """Get Arrow FileSystem object"""
        if mode == "s3":
            fs = pyarrow.fs.S3FileSystem(
                endpoint_override=f"https://{self.region}.{self.endpoint}",
                access_key=self.access_key,
                secret_key=self.secret_key,
            )
        elif mode == "local":
            fs = pyarrow.fs.LocalFileSystem()
        else:
            raise ValueError(f"Mode {mode} is invalid")
        return fs

    def _path_exists(
        self,
        path: str,
        bucket: str | None = None,
    ) -> bool:
        """Validate if the path exists"""
        fs = self._get_arrow_fs(mode="s3" if bucket else "local")
        dir_path = bucket + "/" + path if bucket else path
        return fs.get_file_info(dir_path).type != pyarrow.fs.FileType.NotFound

    def _delete_dataset_if_exists(self, name_or_path: str, bucket: str | None = None) -> None:
        """Delete dataset if exists"""
        dir_path = bucket + "/" + name_or_path if bucket else name_or_path
        fs = self._get_arrow_fs(mode="s3" if bucket else "local")
        file_info = fs.get_file_info(dir_path)
        if file_info.type == pyarrow.fs.FileType.Directory:
            fs.delete_dir(dir_path)
            logger.info("Directory '%s' deleted.", dir_path)
        else:
            logger.info("'%s' is not a directory", dir_path)

    def is_dataset(self, name_or_path: str, bucket: str | None = None) -> bool:
        """Check if a given name/path is a dataset. The deltalake lib is_deltalake method
        somehow created an empty directory when the deltatable doesn't exists. This method is
        a temporary workaround for that.
        """
        fs = self._get_arrow_fs(mode="s3" if bucket else "local")
        dir_path = os.path.join(bucket, name_or_path) if bucket else name_or_path
        log_path = os.path.join(dir_path, "_delta_log")
        file_info = fs.get_file_info(log_path)
        return file_info.type == pyarrow.fs.FileType.Directory

    def _get_table_uri(self, name_or_path: str, bucket: str | None = None) -> str:
        """Get appropriate table uri"""
        if bucket:
            return f"s3://{bucket}/{name_or_path}/"
        return name_or_path

    def get_dataset(
        self, name_or_path: str, bucket: str | None = None, version: int | str | datetime | None = None
    ) -> DeltaTable:
        """Get dataset as DeltaTable or raise DatasetNotFound error"""
        storage_options = self._get_storage_option() if bucket else None
        delta_table_path = self._get_table_uri(name_or_path, bucket)

        if not self.is_dataset(name_or_path, bucket=bucket):
            raise DatasetNotFound(
                f"Dataset {name_or_path} not found at {delta_table_path if bucket else os.path.abspath(delta_table_path)}"
            )

        dt = DeltaTable(delta_table_path, storage_options=storage_options)

        if version is not None:
            dt.load_as_version(version)

        return dt

    def list_datasets(self, dir_path: str | None = None, bucket: str | None = None) -> list[str]:
        """List all dataset names. If dir_path or bucket not provided, this will check
        in the current working directory. dir_path can be provided for local datasets which will be the
        directory to look for dataset. Note that this won't recusively looks into subfolders.
        bucket, if given, will look for dataset in S3-compatible storage
        """
        fs = self._get_arrow_fs(mode="s3" if bucket else "local")
        path = bucket or dir_path or os.getcwd()
        selector = pyarrow.fs.FileSelector(path, recursive=False)

        datasets = []
        for file_info in fs.get_file_info(selector):
            if file_info.type == pyarrow.fs.FileType.Directory:
                _dir_path = f"s3://{bucket}/{file_info.base_name}/" if bucket else file_info.path
                storage_options = self._get_storage_option() if bucket else None
                if DeltaTable.is_deltatable(_dir_path, storage_options=storage_options):
                    datasets.append(file_info.base_name)
        return datasets

    def get_dataset_metadata(self, name_or_path: str, bucket: str | None = None) -> DatasetMetadata:
        """Get dataset metadata"""
        dt = self.get_dataset(name_or_path, bucket)
        metadata = dt.metadata()
        return DatasetMetadata(
            id=str(metadata.id),
            name=metadata.name,
            description=metadata.description,
            created_date=datetime.fromtimestamp(metadata.created_time / 1000),
            schema=dt.schema(),
            partition_columns=metadata.partition_columns,
            constraints={k.split(".")[-1]: v for k, v in metadata.configuration.items()},
        )

    def append(
        self,
        data: pd.DataFrame | pa.Table | ds.Dataset | Path | str,
        dataset_name_or_path: str,
        bucket: str | None = None,
        **kwargs,
    ) -> None:
        """
        Append new data to an existing dataset.

        Args:
            data (pd.DataFrame | pa.Table | ds.Dataset | Path | str): The data to append or path to file.
            dataset_name_or_path (str): The name or path of the dataset.
            bucket (str | None, optional): The name of the S3 bucket. Defaults to None.
            cast_schema (bool, optional): Whether to cast the schema of the data to match the dataset schema. Defaults to False.
        """
        write_data(
            data,
            table_uri=self._get_table_uri(dataset_name_or_path, bucket),
            metadata=self.get_dataset_metadata(dataset_name_or_path, bucket),
            storage_options=self._get_storage_option() if bucket else None,
            mode="append",
            **kwargs,
        )

    def query(self, query: str, delta_read_mode: Literal["pyarrow", "duckdb"] = "pyarrow", **kwargs):
        """Run query string using a query executor. At the moment only duckDB is supported."""
        if self.query_executor is None:
            self.query_executor = DuckDBQueryExecutor(
                self.access_key,
                self.secret_key,
                self.region,
                self.endpoint,
            )

        parsed_query, dataset_refs = self.query_executor.parse_query(query, delta_read_mode)
        for dataset_ref in dataset_refs:
            kwargs[dataset_ref.alias] = self.get_dataset(dataset_ref.name, dataset_ref.bucket).to_pyarrow_dataset()

        return self.query_executor.execute(parsed_query, **kwargs)


def _validate_table_name(table_name: str) -> str:
    """
    Validates a table name. Table name can be alphanumeric and must not contain special characters
    except for - or _, also no space allowed.

    Args:
        table_name (str): The table name to validate.

    Returns:
        Original string if valid or raises DatasetNameNotAllowed
    """
    pattern = re.compile(r"^[a-zA-Z0-9_-]+$")
    if not pattern.match(table_name):
        raise DatasetNameNotAllowed(
            f"Table name '{table_name}' is not allowed. It must be alphanumeric and can contain only '-' or '_'."
        )
    return table_name
