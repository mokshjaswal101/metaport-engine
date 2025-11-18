from typing import Optional
from datetime import datetime
from pydantic import BaseModel

# schema
from schema.base import DBBaseModel
from modules.client.client_schema import ClientModel


class OrderTagsBaseModel(BaseModel):
    name: str
    description: Optional[str] = None
    colour: Optional[str] = None


class OrderTagsModel(OrderTagsBaseModel, DBBaseModel):
    client_id: int
    client: Optional[ClientModel] = None


class OrderTagsAssignmentModel(DBBaseModel):
    order_id: int
    client_id: int
    assigned_at: datetime
