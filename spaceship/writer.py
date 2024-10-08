"""Writer module to write data to delta lake"""

import logging
import tempfile
from abc import (
    ABC,
    abstractmethod,
)
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    IO,
    Any,
    Generator,
    Literal,
)

import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as ds
from deltalake import (
    DeltaTable,
    write_deltalake,
)

from spaceship.metadata import DatasetMetadata

logger = logging.getLogger(__name__)


class SequentialWriter(ABC):
    """Sequencial writer base class"""

    @abstractmethod
    def write(
        self,
        data,
        table_uri: str,
        metadata: DatasetMetadata,
        storage_options: dict,
        mode: Literal["overwrite_partition", "append"],
    ) -> None:
        """Write data to deltalake

        Args:
            data: Data to be written to deltalake table
            table_uri: URI of the delta table
            metadata: Metadata of the delta table
        """


class TableWriter(SequentialWriter):
    """Writer for writting PyArrow table to deltalake"""

    def _add_default_partition(self, data: pa.Table) -> pa.Table:
        """Add default load_partition_date column. This date is based on UTC time"""
        if "load_partition_date" in data.column_names:
            data = data.drop_columns("load_partition_date")
        now_date = datetime.now(timezone.utc).date()
        logger.info("Adding default partition column: load_parition_date with value %s.", now_date)
        load_partition_date = pa.array([now_date] * len(data), pa.date32())
        return data.append_column("load_partition_date", load_partition_date)

    def _cast_schema(self, data: pa.Table, schema: pa.Schema) -> pa.Table:
        """Cast intput table schema to fit deltatable pyarrow schema.
        This will check null constraint defined in the schema
        """
        logger.debug("The data has schema \n %s", data.schema)
        logger.debug("The deltatable has schema \n %s", schema)
        try:
            return data.cast(schema)
        except ValueError:
            logger.exception(
                "Failed to cast input data schema to target delta table. \n"
                "The data has schema \n %s \n The target deltatable has schema %s",
                data.schema,
                schema,
            )
            raise

    def _append(self, data: pa.Table, table_uri: str, storage_options: dict):
        """Append data to deltatable"""
        write_deltalake(
            table_uri,
            data,
            storage_options=storage_options,
            mode="append",
        )

    def write(
        self,
        data: pa.Table,
        table_uri: str,
        metadata: DatasetMetadata,
        storage_options: dict,
        mode: Literal["overwrite_partition", "append"],
    ) -> None:
        """Write data to deltalake"""
        logger.info(
            "Writing a table of size (%s, %s) with the total of %s bytes, mode %s, to %s.",
            data.num_rows,
            data.num_columns,
            data.nbytes,
            mode,
            table_uri,
        )
        if metadata.partition_columns == ["load_partition_date"]:
            data = self._add_default_partition(data)

        data = self._cast_schema(data, metadata.schema.to_pyarrow())

        if mode == "append":
            self._append(data, table_uri, storage_options)
        elif mode == "overwrite_partition":
            raise NotImplementedError()
        else:
            raise ValueError(f"mode {mode} is invalid")


class RecordBatchWriter(TableWriter):
    """Writer to write record batches to delta lake.
    This is a helper class for pyarrow dataset or csv streamreader class
    that can create a record batch generator. This class is designed to handle
    the case when the input data is larger than the available memory.
    """

    def __init__(self, allowed_memory_ratio: float = 0.7):
        self.allowed_memory_ratio = allowed_memory_ratio

    def write(
        self,
        data: Generator[pa.RecordBatch, None, None],
        table_uri: str,
        metadata: DatasetMetadata,
        storage_options: dict,
        mode: Literal["overwrite_partition", "append"],
    ):
        """Write record batches to delta lake"""
        batch_generator = self._split_batches(data)
        batches, done = next(batch_generator)

        if done:
            logger.info("The data can fit in memory. This will be sent as a single pyarrow table.")
            super().write(
                data=pa.Table.from_batches(batches),
                table_uri=table_uri,
                metadata=metadata,
                storage_options=storage_options,
                mode=mode,
            )
            return

        logger.warning(
            "The data is too large to fit in memeory. This will use multipart submission."
            " It could take longer to process."
        )
        with tempfile.TemporaryDirectory() as tmpdirpath:
            self._create_temp_deltatable(path=tmpdirpath, metadata=metadata)
            while batches:
                super().write(
                    data=pa.Table.from_batches(batches),
                    table_uri=tmpdirpath,
                    metadata=metadata,
                    mode="append",
                    storage_options={},
                )
                batches, _ = next(batch_generator)

            self._write_multipart(
                temp_table=DeltaTable(tmpdirpath),
                table_uri=table_uri,
                metadata=metadata,
                storage_options=storage_options,
                mode=mode,
                **kwargs,
            )

    def _write_multipart(
        self,
        temp_table: DeltaTable,
        table_uri: str,
        metadata: DatasetMetadata,
        storage_options: dict,
        mode: Literal["overwrite_partition", "append"],
    ):
        """Helper method to write temp deltatable to target deltatable with ability to roll back if failed to do so"""
        start_version = DeltaTable(table_uri, storage_options=storage_options).version()
        logger.info("Starting multipart submission. Start version: %s", start_version)
        batch_generator = self._split_batches(temp_table.to_pyarrow_dataset().to_batches())
        batches, _ = next(batch_generator)
        i = 0
        try:
            while batches:
                i += 1
                logger.info("Writing part %s", i)
                super().write(
                    data=pa.Table.from_batches(batches),
                    table_uri=table_uri,
                    metadata=metadata,
                    storage_options=storage_options,
                    mode=mode,
                )
                batches, _ = next(batch_generator)
        except BaseException:
            logger.exception("Exception occur while sending multipart. Rolling back to previous version.")
            dt = DeltaTable(table_uri, storage_options=storage_options)
            current_version = dt.version()
            if current_version > start_version:
                logger.info("Restoring from version %s back to %s", current_version, start_version)
                dt.restore(start_version)
                logger.info("The table has been restored.")
            else:
                logger.info("Current version is %s. Nothing to restore.", current_version)
            raise

        logger.info("Finish multipart file sending with the total of %s parts.", i)

    def _split_batches(
        self,
        data: Generator[pa.RecordBatch, None, None],
    ) -> Generator[tuple[list[pa.RecordBatch], bool], None, None]:
        """Split record batches into groups that fit into memory.
        This returns a generator that yields a tuple of 2 things:
            1. list of RecordBatches
            2. Boolean telling if all the data has been exhausted.
        """
        mem_allowed: int = psutil.virtual_memory().available * self.allowed_memory_ratio
        total_bytes: int = 0
        batches = []

        while True:
            try:
                batch = next(data)
                total_bytes += batch.nbytes
                batches.append(batch)
            except StopIteration:
                yield batches, True
                batches = []
                total_bytes = 0

            if total_bytes > mem_allowed:
                yield batches, False
                batches = []
                total_bytes = 0

    def _create_temp_deltatable(self, path: str, metadata: DatasetMetadata) -> None:
        """Create a tmp deltatable locally"""
        logger.info("Creating a temp delta table at %s", path)
        dt = DeltaTable.create(
            table_uri=path, schema=metadata.schema.to_pyarrow(), mode="error", partition_by=metadata.partition_columns
        )
        if metadata.constraints:
            dt.alter.add_constraint(metadata.constraints)


def get_writer(data: Any, **kwargs) -> SequentialWriter:
    """Factory function to get a writer"""
