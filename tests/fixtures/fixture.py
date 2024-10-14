"""Example fixtures."""

from datetime import (
    date,
    datetime,
)
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from deltalake import (
    DeltaTable,
    write_deltalake,
)

from spaceship.client import Client
from spaceship.metadata import DatasetMetadata


def create_tmp_dataset(path: str) -> DeltaTable:
    """Create a tmp deltatable with some dummy data"""
    DeltaTable.create(
        table_uri=path,
        schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
        description="Some description",
        mode="error",
        partition_by=["date"],
    )
    dtypes = {"id": "int64[pyarrow]", "date": "date32[pyarrow]", "name": "string[pyarrow]"}
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "name": ["Alice", "Bob", "Charlie"],
        }
    ).astype(dtypes)
    write_deltalake(path, df, mode="append")
    return DeltaTable(path)


@pytest.fixture(scope="function")
def schema() -> pa.Schema:
    """PyArrow Schema for testing"""
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("date", pa.date32(), nullable=False),
            pa.field("amount", pa.int64(), nullable=False),
            pa.field("city", pa.string(), nullable=False),
        ]
    )


@pytest.fixture(scope="function")
def deltatable(tmp_path: Path, schema: pa.Schema) -> DeltaTable:
    """Temp DeltaTable"""
    path = tmp_path / "tmp_table"
    DeltaTable.create(
        table_uri=path, schema=schema, description="Some description", mode="error", partition_by=["date"]
    )
    return DeltaTable(path)


@pytest.fixture(scope="function")
def dataset_metadata(deltatable: DeltaTable) -> DatasetMetadata:
    """Temp DeltaTable"""
    metadata = deltatable.metadata()
    return DatasetMetadata(
        id=str(metadata.id),
        name=metadata.name,
        description=metadata.description,
        created_date=datetime.fromtimestamp(metadata.created_time / 1000),
        schema=deltatable.schema(),
        partition_columns=metadata.partition_columns,
        constraints={k.split(".")[-1]: v for k, v in metadata.configuration.items()},
    )


@pytest.fixture(scope="function")
def pandas_df() -> pd.DataFrame:
    """Pandas DataFrame fixture for testing"""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "category": ["Electronics", "Clothing", "Grocery", "Electronics", "Clothing"],
            "date": map(date.fromisoformat, ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03", "2024-01-04"]),
            "amount": [150, 85, 120, 300, 200],
            "city": ["New York", "San Francisco", "Chicago", "New York", "San Francisco"],
        }
    )


@pytest.fixture(scope="function")
def invalid_schema_pandas_df() -> pd.DataFrame:
    """Pandas DataFrame fixture for testing"""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", None, "Eve"],
            "category": ["Electronics", "Clothing", "Grocery", "Electronics", "Clothing"],
            "date": map(date.fromisoformat, ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03", "2024-01-04"]),
            "amount": [150.13, 85, 120, 300, 200],
            "city": ["New York", "San Francisco", "Chicago", "New York", "San Francisco"],
        }
    )


@pytest.fixture(scope="function")
def pyarrow_table(pandas_df: pd.DataFrame) -> pa.Table:
    """PyArrow Table fixture for testing"""
    return pa.Table.from_pandas(pandas_df)


@pytest.fixture(scope="function")
def parquet_file(pandas_df: pd.DataFrame, tmp_path: Path) -> str:
    """Parquet file fixture for testing. This returns the path to the file"""
    file_path = tmp_path / "test_data.parquet"
    pandas_df.to_parquet(file_path)
    return str(file_path)


@pytest.fixture(scope="function")
def csv_file(pandas_df: pd.DataFrame, tmp_path: Path) -> str:
    """Text file fixture for testing. This returns the path to the file"""
    file_path = tmp_path / "test_data.csv"
    pandas_df.to_csv(file_path, index=False)
    return str(file_path)


@pytest.fixture(scope="function")
def pyarrow_dataset(parquet_file: str) -> ds.Dataset:
    """Test PyArrow dataset fixture for testing"""
    return ds.dataset(parquet_file)


@pytest.fixture(scope="function")
def client() -> Client:
    """Local client fixture"""
    return Client(
        access_key="test_access_key", secret_key="test_secret_key", region="test_region", endpoint="test_endpoint"
    )
