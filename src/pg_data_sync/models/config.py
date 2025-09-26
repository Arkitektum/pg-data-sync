from typing import List, Optional
from pydantic import BaseModel
from .dataset_config import DatasetConfig
from .indexing_config import IndexingConfig


class Config(BaseModel):
    dataset: DatasetConfig
    indexing: Optional[List[IndexingConfig]]
