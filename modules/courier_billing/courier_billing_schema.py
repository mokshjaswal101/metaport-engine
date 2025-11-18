from typing import Optional
from decimal import Decimal

from pydantic import BaseModel

from schema.base import DBBaseModel


class CourierBillingCreateRequest(BaseModel):
    """Schema for creating a basic courier billing record"""

    order_id: int
    awb_number: Optional[str] = None
    final_freight: Optional[Decimal] = None
    final_tax: Optional[Decimal] = None
    calculated_freight: Optional[Decimal] = None
    calculated_tax: Optional[Decimal] = None


class CourierBillingModel(DBBaseModel):
    """Basic courier billing model for responses"""

    order_id: int
    awb_number: Optional[str] = None
    final_freight: Optional[Decimal] = None
    final_tax: Optional[Decimal] = None
    calculated_freight: Optional[Decimal] = None
    calculated_tax: Optional[Decimal] = None

    class Config:
        from_attributes = True


class CourierBillingListResponse(BaseModel):
    """Schema for listing courier billing records"""

    billing_records: list[CourierBillingModel]
    total_count: int
