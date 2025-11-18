from pydantic import BaseModel
from datetime import datetime
from uuid import UUID
from typing import Optional, Any


# Generic response model for all responses
class GenericResponseModel(BaseModel):
    status_code: int
    message: str = None
    status: bool = False
    data: Any = {}


# Base model for all models that will be stored in the database
class DBBaseModel(BaseModel):
    id: int
    uuid: UUID
    created_at: datetime
    updated_at: Optional[datetime]
    is_deleted: bool = False

    class Config:
        from_attributes = True
