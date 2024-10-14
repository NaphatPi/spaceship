"""Writer module to write data to delta lake"""

import gc
import logging
import os
import tempfile
import time
from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    Generator,
    Iterable,
    Literal,
)

import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from deltalake import (
    DeltaTable,
    write_deltalake,
)

from spaceship.exception import RestorationError
from spaceship.metadata import DatasetMetadata
from spaceship.utils import get_valid_argument

logger = logging.getLogger(__name__)


NOW_DATE = datetime.now(timezone.utc).date()
ALLOWED_MEMORY_RATIO = float(os.getenv("ALLOWED_MEMORY_RATIO") or 0.3)


def _table_generator(
    data: Iterable[pa.RecordBatch], allowed_memory_ratio: float = ALLOWED_MEMORY_RATIO
) -> Generator[tuple[pa.Table, bool], None, None]:
    """This generator splits record batches into groups that fit into memory.

    Args:
        data: generator returning record batches
        allowed_memory_ratio: ratio of memory allowed as max size of a table yielded
    This returns a generator that yields a tuple of 2 things:
        1. list of RecordBatches
        2. Boolean telling if all the data has been exhausted.
    """
    mem_allowed: int = psutil.virtual_memory().available * allowed_memory_ratio
    logger.info("Generating batch groups with max memory of %s bytes per group", mem_allowed)
    total_bytes: int = 0
    batches: list[pa.RecordBatch] = []

    for batch in data:
        if total_bytes >= mem_allowed:
            table, batches = pa.Table.from_batches(batches), []
            yield table, False
            total_bytes = 0
        batches.append(batch)
        total_bytes += batch.nbytes

    yield pa.Table.from_batches(batches), True


def _add_default_partition(data: pa.Table) -> pa.Table:
    """Add default load_partition_date column. This date is based on UTC time"""
    if "load_partition_date" in data.column_names:
        data = data.drop_columns("load_partition_date")
    logger.info("Adding default partition column: load_parition_date with value %s.", NOW_DATE)
    load_partition_date = pa.array([NOW_DATE] * len(data), pa.date32())
    return data.append_column("load_partition_date", load_partition_date)


def _cast_schema(data: pa.Table, schema: pa.Schema) -> pa.Table:
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


def _create_temp_deltatable(path: str, metadata: DatasetMetadata) -> DeltaTable:
    """Create a tmp deltatable locally"""
    logger.info("Creating a temp delta table at %s", path)
    dt = DeltaTable.create(
        table_uri=path, schema=metadata.schema.to_pyarrow(), mode="error", partition_by=metadata.partition_columns
    )
    if metadata.constraints:
        dt.alter.add_constraint(metadata.constraints)
    return dt


def _append_table(
    data: pa.Table,
    table_uri: str,
    metadata: DatasetMetadata,
    storage_options: dict,
):
    """Append pyarrow table to target deltatable"""
    logger.info(
        "Appending a table of size (%s, %s) with the total of %s bytes, to %s.",
        data.num_rows,
        data.num_columns,
        data.nbytes,
        table_uri,
    )

    if metadata.partition_columns == ["load_partition_date"]:
        data = _add_default_partition(data)

    data = _cast_schema(data, metadata.schema.to_pyarrow())
    write_deltalake(
        table_uri,
        data,
        storage_options=storage_options,
        mode="append",
    )


def _restore_deltatable(
    table_uri: str,
    to_version: int,
    storage_options: dict[str, str] | None,
    max_attempts: int = 3,
    attempt_interval: int = 60,
):
    """Restore delta table to the target version"""
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        logger.info(
            "Trying to revert deltatable %s to version %s. Attemp %s of %s",
            table_uri,
            to_version,
            attempt,
            max_attempts,
        )
        try:
            dt = DeltaTable(table_uri, storage_options=storage_options)
            current_version = dt.version()
            if current_version > to_version:
                logger.warning("Restoring from version %s back to %s", current_version, to_version)
                dt.restore(to_version)
                logger.warning("The table has been restored.")
            else:
                logger.warning("Current version is already %s. Nothing to restore.", current_version)
            return
        except Exception as ex:
            logger.error("Exception occur: %s. Will retry in %s seconds", ex, attempt_interval)
            time.sleep(attempt_interval)

    raise RestorationError(
        "Failed to restore the deltatable. This might be from network error. Please check the tracback for more info."
    )


def _append_multi_tables(
    data: Iterable[pa.Table],
    table_uri: str,
    metadata: DatasetMetadata,
    storage_options: dict,
):
    """Helper method to append tables to target deltatable with ability to roll back if failed to do so
    This is
    """
    start_version = DeltaTable(table_uri, storage_options=storage_options).version()
    logger.info("Starting multipart submission. Start version: %s", start_version)
    try:
        for i, (table, _) in enumerate(data, 1):
            logger.info("Writing part %s", i)
            _append_table(
                data=table,
                table_uri=table_uri,
                metadata=metadata,
                storage_options=storage_options,
            )
    except BaseException:
        logger.exception("Exception occur while sending multipart. Rolling back to previous version.")
        _restore_deltatable(table_uri, start_version, storage_options)
        raise

    logger.info("Finish multipart file sending with the total of %s parts.", i)


def _append(
    data: Iterable[pa.RecordBatch],
    table_uri: str,
    metadata: DatasetMetadata,
    storage_options: dict[str, str] | None,
) -> None:
    """Append data to delta table"""
    table_generator = _table_generator(data)
    table, done = next(table_generator)

    if done:
        logger.info("The data can fit in memory. This will be sent as a single pyarrow table.")
        _append_table(
            data=table,
            table_uri=table_uri,
            metadata=metadata,
            storage_options=storage_options,
        )
        return

    logger.warning(
        "The data is too large to fit in memeory. This will use multipart submission."
        " It could take longer to process."
    )
    with tempfile.TemporaryDirectory() as tmpdirpath:
        _create_temp_deltatable(path=tmpdirpath, metadata=metadata)
        while True:
            _append_table(
                data=table,
                table_uri=tmpdirpath,
                metadata=metadata,
                storage_options={},
            )
            if done:
                break
            table, done = next(table_generator)

        # Delete variable and reclaim memory before seding multipart
        del table
        gc.collect()

        table_generator = _table_generator(DeltaTable(tmpdirpath).to_pyarrow_dataset().to_batches())
        _append_multi_tables(
            data=table_generator,
            table_uri=table_uri,
            metadata=metadata,
            storage_options=storage_options,
        )


def _overwrite_partition():
    """Send data to deltatable overwriting partitions that matches"""
    raise NotImplementedError()


def _process_table(data: pa.Table, **kwargs) -> Iterable[pa.RecordBatch]:
    """Process pyarrow table to RecordBatches"""
    kwargs = get_valid_argument(kwargs, pa.Table.to_batches)
    return data.to_batches(**kwargs)


def _process_dataframe(data: pd.DataFrame, **kwargs) -> Iterable[pa.RecordBatch]:
    """Process Pandas dataframe to RecordBatches"""
    return pa.Table.from_pandas(data, **get_valid_argument(kwargs, pa.Table.from_pandas)).to_batches(
        **get_valid_argument(kwargs, pa.Table.to_batches)
    )


def _process_dataset(data: ds.Dataset, **kwargs) -> Iterable[pa.RecordBatch]:
    """Process PyArrow dataset to RecordBatches"""
    kwargs = get_valid_argument(kwargs, ds.Dataset.to_batches)
    return data.to_batches(**kwargs)


def _process_text_file(path: str | Path, **kwargs) -> Iterable[pa.RecordBatch]:
    """Process text file to RecordBatches"""
    kwargs = get_valid_argument(kwargs, csv.open_csv)
    return csv.open_csv(path, **kwargs)


def _process_parquet_file(path: str | Path, **kwargs) -> Iterable[pa.RecordBatch]:
    """Process text file to RecordBatches"""
    parquet_file = pq.ParquetFile(path, **get_valid_argument(kwargs, pq.ParquetFile))
    return parquet_file.iter_batches(**get_valid_argument(kwargs, pq.ParquetFile.iter_batches))


def write_data(
    data: pd.DataFrame | pa.Table | ds.Dataset | Iterable[pa.RecordBatch] | str | Path,
    *,
    table_uri: str,
    metadata: DatasetMetadata,
    storage_options: dict[str, str] | None = None,
    mode: Literal["append", "overwrite_partition"],
    **kwargs,
):
    """Generic function to write data to deltatable"""
    if isinstance(data, pd.DataFrame):
        data = _process_dataframe(data, **kwargs)
    elif isinstance(data, pa.Table):
        data = _process_table(data, **kwargs)
    elif isinstance(data, ds.Dataset):
        data = _process_dataset(data, **kwargs)
    elif isinstance(data, (str, Path)):
        if not Path(data).is_file():
            raise ValueError(f"path {data} is not a file or not found")

        ext = Path(data).suffix
        if ext in (".csv", ".txt", ".dat"):
            data = _process_text_file(data, **kwargs)
        elif ext == ".parquet":
            data = _process_parquet_file(data, **kwargs)
        else:
            raise ValueError(f"File extension '{ext}' not supported.")
    else:
        raise TypeError(f"Unsupported datatype {type(data)}")

    if mode == "append":
        _append(data, table_uri=table_uri, metadata=metadata, storage_options=storage_options)
    elif mode == "overwrite_partition":
        raise NotImplementedError()
    else:
        raise ValueError(f"Invalid mode '{mode}'")
