"""Testing Client module"""

from datetime import (
    datetime,
    timezone,
)
from pathlib import Path
from unittest.mock import (
    Mock,
    patch,
)

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import (
    DeltaTable,
    Schema,
    write_deltalake,
)

from spaceship.client import Client
from spaceship.exception import (
    DatasetAlreadyExists,
    DatasetNameNotAllowed,
    DatasetNotFound,
)
from spaceship.query import FutureDatasetRef


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


def test_create_dataset_locally(client: Client, tmp_path: Path):
    """Test creating a dataset"""
    dataset_path = tmp_path / "test_dataset"
    client.create_dataset(
        str(dataset_path),
        schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
        description="test dataset",
        partition_columns=["date"],
        constraints={
            "id_not_null": "id IS NOT NULL",
        },
    )
    assert Path.is_dir(dataset_path)
    assert DeltaTable.is_deltatable(str(dataset_path))


def test_create_dataset_failure_already_exists(client: Client, tmp_path: Path):
    """Test creating a dataset with already exists directory"""
    Path.mkdir(tmp_path / "test_dataset")
    dataset_path = tmp_path / "test_dataset"
    with pytest.raises(DatasetAlreadyExists):
        client.create_dataset(
            str(dataset_path),
            schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
            description="test dataset",
            partition_columns=["date"],
            constraints={
                "id_not_null": "id IS NOT NULL",
            },
        )


def test_create_dataset_with_invalid_name(client: Client, tmp_path: Path):
    """Test creating a dataset with an invalid name"""
    dataset_path = tmp_path / "!some name"
    with pytest.raises(DatasetNameNotAllowed):
        client.create_dataset(
            str(dataset_path),
            schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
            description="test dataset",
            partition_columns=["date"],
            constraints={
                "id_not_null": "id IS NOT NULL",
            },
        )


def test_delete_dir_after_failing_to_create(client: Client, tmp_path: Path):
    """Test if partially created dataset is deleted"""
    dataset_path = tmp_path / "test_dataset"
    with pytest.raises(Exception):
        client.create_dataset(
            str(dataset_path),
            schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
            description="test dataset",
            partition_columns=["date"],
            constraints={
                "id_not_null": "some invalid statement",
            },
        )
    assert not dataset_path.exists()


def test_get_existing_dataset(client: Client, tmp_path: Path):
    """Test getting dataset"""
    table_path = str(tmp_path / "tmp_table")
    dt1 = create_tmp_dataset(table_path)
    dt2 = client.get_dataset(table_path)
    assert dt1.to_pandas().equals(dt2.to_pandas())


def test_get_not_existing_dataset(client: Client, tmp_path: Path):
    """Test getting dataset that does not exists"""
    with pytest.raises(DatasetNotFound):
        client.get_dataset(str(tmp_path / "some_random_dataset"))


def test_list_datasets(tmp_path: Path, client: Client):
    """Test list datasets"""
    datasets = ["ds1, ds2"]
    for ds in datasets:
        create_tmp_dataset(str(tmp_path / ds))
    assert client.list_datasets(str(tmp_path)) == datasets

    random_path = tmp_path / "random_dir"
    Path.mkdir(random_path)
    assert client.list_datasets(str(random_path)) == []


def test_is_dataset(client: Client, tmp_path: Path):
    """Test if checking for dataset is valid"""
    dataset_path = str(tmp_path / "test-dataset")
    create_tmp_dataset(dataset_path)
    assert client.is_dataset(dataset_path)
    assert not client.is_dataset(str(tmp_path / "random-dataset"))


def test_dataset_metadata(client: Client, tmp_path: Path):
    """Test dataset metadata"""
    name = "some-dataset"
    fields = [
        pa.field("id", pa.int64(), nullable=False, metadata={"description": "user id"}),
        pa.field("date", pa.date32(), nullable=False, metadata={"description": "register date"}),
        pa.field("name", pa.string(), nullable=True, metadata={"description": "user name"}),
    ]
    schema = pa.schema(fields)
    description = "some description"
    partition_columns = ["date"]
    constraints = {"id_not_null": "id IS NOT NULL"}

    client.create_dataset(
        str(tmp_path / name),
        schema=schema,
        description=description,
        partition_columns=partition_columns,
        constraints=constraints,
    )
    metadata = client.get_dataset_metadata(str(tmp_path / name))

    assert metadata.name == name
    assert metadata.description == description
    assert metadata.schema == Schema.from_pyarrow(schema)
    assert metadata.constraints == constraints
    assert metadata.created_date.date() == datetime.now().date()
    assert metadata.partition_columns == partition_columns


def test_append_data(client: Client, tmp_path: Path):
    """Test appending to a dataset"""
    name = "some-dataset"
    fields = [
        pa.field("id", pa.int64(), nullable=False, metadata={"description": "user id"}),
        pa.field("date", pa.date32(), nullable=False, metadata={"description": "register date"}),
        pa.field("name", pa.string(), nullable=True, metadata={"description": "user name"}),
    ]
    DeltaTable.create(
        table_uri=str(tmp_path / name),
        schema=pa.schema(fields),
        description="Some description",
        mode="error",
        partition_by=["date"],
    )

    dtypes = {"id": "int64[pyarrow]", "date": "date32[pyarrow]", "name": "string[pyarrow]"}

    df1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "name": ["Alice", "Bob", "Charlie"],
        }
    ).astype(dtypes)
    df2 = pd.DataFrame(
        {
            "id": [4, 5, 6],
            "date": pd.to_datetime(["2023-01-04", "2023-01-05", "2023-01-06"]),
            "name": ["Mike", "Jeff", "Nolan"],
        }
    ).astype(dtypes)

    client.append(df1, str(tmp_path / name))

    assert df1.equals(
        DeltaTable(str(tmp_path / name)).to_pandas().sort_values("id").reset_index(drop=True).astype(dtypes)
    )

    client.append(df2, str(tmp_path / name))

    assert (
        pd.concat([df1, df2])
        .reset_index(drop=True)
        .equals(DeltaTable(str(tmp_path / name)).to_pandas().sort_values("id").reset_index(drop=True).astype(dtypes))
    )


def test_create_dataset_with_default_partition_column(client: Client, tmp_path: Path):
    """Test creating a dataset"""
    dataset_path = str(tmp_path / "test_dataset")
    client.create_dataset(
        dataset_path,
        schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
        description="test dataset",
        constraints={
            "id_not_null": "id IS NOT NULL",
        },
    )
    metadata = client.get_dataset_metadata(dataset_path)
    assert metadata.partition_columns == ["load_partition_date"]


def test_append_data_to_dataset_with_default_partition(client: Client, tmp_path: Path):
    """Testing if appending data to dataset with default partition column will have load partition date column"""
    dataset_path = str(tmp_path / "test_dataset")
    client.create_dataset(
        str(dataset_path),
        schema=pa.schema([("id", pa.int64()), ("date", pa.date32()), ("name", pa.string())]),
        description="test dataset",
        constraints={
            "id_not_null": "id IS NOT NULL",
        },
    )
    dtypes = {"id": "int64[pyarrow]", "date": "date32[pyarrow]", "name": "string[pyarrow]"}
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "name": ["Alice", "Bob", "Charlie"],
        }
    ).astype(dtypes)

    client.append(df, dataset_path)

    new_data = DeltaTable(dataset_path).to_pandas()
    assert "load_partition_date" in new_data.columns
    assert len(new_data.load_partition_date.unique()) == 1
    assert new_data.load_partition_date[0] == datetime.now(timezone.utc).date()


@pytest.mark.parametrize(
    "dataset_refs",
    [
        [],
        [FutureDatasetRef("ds1", "f_ds1", "bucket1")],
        [FutureDatasetRef("ds1", "f_ds1", "bucket1"), FutureDatasetRef("ds2", "f_ds2", None)],
    ],
)
def test_pyarrow_dataset_id_provided(client: Client, dataset_refs: list[FutureDatasetRef]):
    """Test if client provided pyarrow dataset when required by the query"""
    mock_query_exe = Mock()
    mock_query_exe.parse_query.return_value = "some_query", dataset_refs
    mock_query_exe.execute.return_value = None
    client.query_executor = mock_query_exe

    with patch("spaceship.client.Client.get_dataset") as mock_get_dataset_method:

        def deltatable(name, _):
            mock_deltatable = Mock()
            mock_deltatable.name = name
            mock_deltatable.to_pyarrow_dataset.return_value = name
            return mock_deltatable

        mock_get_dataset_method.side_effect = deltatable

        client.query("some query")

        kwargs = {ds_ref.alias: ds_ref.name for ds_ref in dataset_refs}

        mock_query_exe.execute.assert_called_once_with("some_query", **kwargs)
