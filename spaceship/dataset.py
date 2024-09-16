"""Dataset module"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd  # type: ignore
from pydantic import (
    BaseModel,
    ConfigDict,
)

if TYPE_CHECKING:
    from spaceship.client import BaseClient


class Dataset(BaseModel):
    """Dataset class"""

    model_config = ConfigDict(use_enum_values=True, frozen=True)

    name: str
    description: str
    tags: list[str] = []
    author: str | None
    contact: str | None
    created_at: datetime
    partition_columns: list[str] = []
    partition_style: str = "hive"
    pandera_schema: dict
    file_format: str = "parquet"
    retention_repriod: int | None = None

    def append(self, data: pd.DataFrame | str, client: BaseClient):
        """Append data to the dataset"""

    def overwrite_partition(self, data: pd.DataFrame | str, client: BaseClient):
        """
        Overwrite data in the dataset partitions.
        This will only overwrite partition that only show up in the input data
        """
