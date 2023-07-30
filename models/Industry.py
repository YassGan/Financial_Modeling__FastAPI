from pydantic import BaseModel, Field
from typing import Optional




class Industry(BaseModel):
    sectorId: Optional[str] = Field(None)
    name: Optional[str] = Field(None)
    name: str
    image: str