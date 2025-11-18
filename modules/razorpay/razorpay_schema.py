from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime


# schema
from schema.base import DBBaseModel
from modules.client.client_schema import ClientModel


class RazorPayValidateRequest(BaseModel):
    order_id: str
    payment_id: str


class PaymentRecord(BaseModel):
    gateway: str
    payment_id: Optional[str]
    order_id: str
    status: str
    amount: float
    currency: str
    method: Optional[str]
    type: str
    client_id: int

    model_config = ConfigDict(from_attributes=True)


class PaymentRecordResponseModel(PaymentRecord):
    created_at: datetime


class PaymentRecordModel(PaymentRecord, DBBaseModel):
    client: Optional[ClientModel]
