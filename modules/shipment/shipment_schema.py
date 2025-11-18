from pydantic import BaseModel, Json
from sqlalchemy.dialects.postgresql import JSONB
from typing import List, Optional
from datetime import datetime

# schema
from schema.base import DBBaseModel


class CreateShipmentModel(BaseModel):
    order_id: str
    contract_id: int
    total_freight: Optional[float] = None
    cod_freight: Optional[float] = None
    tax: Optional[float] = None


class AutoCreateShipmentModel(BaseModel):
    order_id: str
    priority_type: str


class BulkCreateShipmentModel(BaseModel):
    order_ids: List[str]
    courier_id: int


class NewBulkCreateShipmentModel(BaseModel):
    order_ids: List[str]
    courier_id: Optional[int] = None
    courier_type: str


class generateLabelRequest(BaseModel):
    order_ids: List[str]


class ShippingChargesSchema(BaseModel):
    order_id: str
    sub_status: str
    awb_number: str
    order_date: datetime
    payment_mode: str
    forward_freight: Optional[float]
    forward_cod_charge: Optional[float]
    forward_tax: Optional[float]
    rto_freight: Optional[float]
    rto_tax: Optional[float]
    courier_partner: str


class ShippingChargesGetSchema(BaseModel):
    batchSize: int
    selectedPageNumber: int


class CancelshipmentRequestSchema(BaseModel):
    awb_numbers: List[str]
