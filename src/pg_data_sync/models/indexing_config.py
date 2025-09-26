from typing import List, Optional
from pydantic import BaseModel


class IndexingConfig(BaseModel):
    dbs: List[str]
    schemas: List[str] = ['public']
    tables: List[str]
    id_column: str
    geom_index: bool = True
    indexes: Optional[List[List[str]]] = []
