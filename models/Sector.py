from pydantic import BaseModel, Field
from typing import Optional




class Sector(BaseModel):
    name: str
