from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime


# schema
from schema.base import DBBaseModel
from modules.client.client_schema import ClientModel


class PaymentRequest(BaseModel):
    amount: float
    firstname: str
    email: str
    phone: Optional[str] = None
    product_info: str
    return_url: Optional[str] = None


class PaymentResponse(BaseModel):
    payment_url: str
    payment_params: dict
