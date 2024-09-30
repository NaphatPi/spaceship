"""Metadata module"""

from dataclasses import dataclass
from datetime import datetime

from deltalake import Schema


@dataclass
class DatasetMetadata:
    """Dataset metadata dataclass"""

    id: str
    name: str
    description: str
    schema: Schema
    constraints: dict[str, str]
    partition_columns: list[str]
    created_date: datetime
