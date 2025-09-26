from typing import Optional
from pydantic import BaseModel


class FileMap(BaseModel):
    glob: Optional[str] = None
    db_name: str
    db_schema: Optional[str] = None
    db_role: Optional[str] = None
    db_role_pwd: Optional[str] = None
