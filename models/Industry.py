from pydantic import BaseModel, Field
from typing import Optional




class Industry(BaseModel):
    sectorId: str
    name: str
    image: str