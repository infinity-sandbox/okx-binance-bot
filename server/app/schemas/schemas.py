from typing import Optional
from uuid import UUID
from pydantic import BaseModel

class TestSchema(BaseModel):
    test: str
