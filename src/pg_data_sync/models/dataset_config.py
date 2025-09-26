from uuid import UUID
from typing import List, Dict, Any
from pydantic import BaseModel, ConfigDict
from .file_map import FileMap
from .enums import AreaType, Format


class DatasetConfig(BaseModel):
    metadata_id: UUID
    area_code: str
    area_type: AreaType
    epsg: str
    format: Format
    files: List[FileMap]

    model_config = ConfigDict(
        coerce_numbers_to_str=True
    )

    def create_order_request_body(self) -> Dict[str, Any]:
        return {
            'orderLines': [
                {
                    'metadataUuid': str(self.metadata_id),
                    'areas': [
                        {
                            'code': self.area_code,
                            'type': self.area_type
                        }
                    ],
                    'projections': [
                        {
                            'code': self.epsg
                        }
                    ],
                    'formats': [
                        {
                            'name': self.format
                        }
                    ]
                }
            ]
        }
