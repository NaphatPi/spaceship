"""Client module"""

import logging
import os
from pathlib import Path
from typing import Self

from pyarrow.fs import (  # type: ignore
    FileSelector,
    FileSystem,
    LocalFileSystem,
    S3FileSystem,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    model_validator,
)

from spaceship.dataset import Dataset
from spaceship.exception import (
    DatasetNotFound,
    MetadataNotFound,
)

logger = logging.getLogger(__name__)


class BaseClient(BaseModel):
    """Base Client class"""

    model_config = ConfigDict(use_enum_values=True, frozen=True, validate_default=True)

    _filesystem: FileSystem
    _root_path: str

    def create_dataset(self):
        """Create a new dataset"""

    def list_datasets(self) -> list[str]:
        """Get a list of dataset names within root_path"""
        datasets = []
        selector = FileSelector(self._root_path, recursive=False)
        for file_info in self._filesystem.get_file_info(selector):
            logger.debug("Found item: %s", file_info)
            if file_info.type.name == "Directory":
                metadata_path = os.path.join(file_info.path, "metadata.json")
                if self._filesystem.get_file_info(metadata_path).type.name == "File":
                    datasets.append(Path(file_info.path).stem)
        return datasets

    def get_dataset(self, name: str) -> Dataset:
        """Get dataset instance"""
        dataset_path = os.path.join(self._root_path, name)
        metadata_path = os.path.join(dataset_path, "metadata.json")

        if self._filesystem.get_file_info(dataset_path).type.name != "Directory":
            raise DatasetNotFound(f"Dataset '{name}' not found in '{self._root_path}'")

        if self._filesystem.get_file_info(metadata_path).type.name != "File":
            raise MetadataNotFound(f"Dataset metadata not found in {dataset_path}")

        with self._filesystem.open_input_file(metadata_path) as f:
            return Dataset.model_validate_json(f.read())


class RemoteClient(BaseClient):
    """Remote Client Class for managing datasets in a s3-compatible bucket

    Args:
        bucket_name: bucket name e.g. 'my-bucket'
        s3_region: region of the s3 bucket e.g. 'nyc3'
        region_url: for non-S3 provider such as Spaces, provide region url here instead.
            e.g. https://nyc3.digitaloceanspaces.com
        access_key: bucket access key. This can be set with env var BUCKET_ACCESS_KEY
        secret_key: bucket secret key. This can be set with env var BUCKET_SECRET_KEY

    """

    bucket_name: str
    s3_region: str | None = None
    region_url: str | None = None
    access_key: str | None = os.getenv("BUCKET_ACCESS_KEY")
    secret_key: str | None = os.getenv("BUCKET_SECRET_KEY")

    @model_validator(mode="after")
    def after_validation(self) -> Self:
        if self.s3_region is None and self.region_url is None:
            raise ValueError("At least one of s3region or region_url need to be provided.")

        self._root_path = self.bucket_name
        self._filesystem = S3FileSystem(
            region=self.s3_region,
            endpoint_override=self.region_url or None,
            access_key=self.access_key,
            secret_key=self.secret_key,
        )
        return self


class LocalClient(BaseClient):
    """Local Client Class for managing datasets in the local filesystem

    Args:
        root_path: path to root directory containing datasets

    """

    root_path: str

    @model_validator(mode="after")
    def after_validation(self) -> Self:
        self._root_path = self.root_path
        self._filesystem = LocalFileSystem()
        return self
