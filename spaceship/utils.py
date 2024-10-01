"""Utilities module"""

from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from spaceship.metadata import DatasetMetadata


def add_load_partition_date_column(data: pa.Table) -> pa.Table:
    """Add load_partition_date column to the data"""
    load_partition_date = pa.array([datetime.utcnow().date()] * len(data), pa.date32())
    return data.append_column("load_partition_date", load_partition_date)


def validate_input_data_type(data: pd.DataFrame | pa.Table | ds.Dataset) -> pa.Table:
    """Validate input data. Data must be of type pd.DataFrame, pa.Table or ds.Dataset"""
    if isinstance(data, pd.DataFrame):
        tb = pa.Table.from_pandas(data)
    elif isinstance(data, ds.Dataset):
        tb = data.to_table()
    elif isinstance(data, pa.Table):
        tb = data
    else:
        raise TypeError(f"Data must be of type pd.DataFrame, pa.Table or ds.Dataset, not {type(data).__name__}")
    return tb


def compare_data_with_metadata(data: pa.Table, metadata: DatasetMetadata) -> pa.Table:
    """Compare data (pyarrow table) with the dataset metdata. This only includes partition columns validation for now"""
    if metadata.partition_columns == ["load_partition_date"]:
        # This is a default partition column to be added
        data = add_load_partition_date_column(data)

    return data
