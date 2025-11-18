from pydantic import BaseModel, Json
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional

# schema
from schema.base import DBBaseModel


class ServiceabilityParamsModel(BaseModel):
    delivery_pincode: int
    pickup_pincode: int
    order_id: Optional[str]
    shipment_type: Optional[str] = "forward"


class CourierServiceabilityResponseModel(BaseModel):
    name: str
    mode: str
    courier_id: int
    logo: Optional[str]
    slug: str
    tax_amount: float
    freight: float
    cod_charges: float
    chargeable_weight: float
    min_chargeable_weight: float
    additional_weight_bracket: float


class RateCalculatorParamsModel(BaseModel):
    delivery_pincode: str
    pickup_pincode: str
    shipment_type: Optional[str] = "forward"
    actualWeight: int
    length: Optional[float] = None
    breadth: Optional[float] = None
    height: Optional[float] = None
    shipment_value: Optional[float] = 0.0
    paymentType: str


class RateCalculatorResponseModel(BaseModel):
    courier_name: str
    courier_id: int
    mode: str
    logo: Optional[str] = None
    slug: str
    base_freight: float
    tax_amount: float
    cod_charges: float
    total_freight: float
    chargeable_weight: float
    min_chargeable_weight: float
    volumetric_weight: Optional[float] = None
