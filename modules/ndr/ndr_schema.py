from enum import Enum
from uuid import UUID
from typing import Optional, Any, List, Dict
from datetime import datetime

from pydantic import BaseModel

# schema
# from schema.base import DBBaseModel


class Ndr_filters(BaseModel):
    batch_size: int
    page_number: int
    ndr_status: str
    start_date: datetime
    end_date: datetime
    search_term: Optional[str] = ""


class Ndr_reattempt_escalate(BaseModel):
    alternatePhoneNumber: Optional[str] = None
    address: Optional[str] = None
    uuid: str


class Ndr_reattempt_escalate(BaseModel):
    alternatePhoneNumber: Optional[str] = None
    address: Optional[str] = None
    uuid: str


class Bulk_Ndr_reattempt_escalate(BaseModel):
    order_ids: List[str]


class Ndr_status_update(BaseModel):
    uuid: str


class Ndr_Response_Model(BaseModel):
    awb: Optional[str]
    status: Optional[str]
    datetime: Optional[str]
    reason: Optional[int]

    # class Config:
    #     from_attributes = True  # Enables compatibility with ORM models


class Ndr_reattempt_escalate(BaseModel):
    alternatePhoneNumber: Optional[str] = None
    address: Optional[str] = None
    uuid: str


class Bulk_Ndr_reattempt_escalate(BaseModel):
    order_ids: List[str]
