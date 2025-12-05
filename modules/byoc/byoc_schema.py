from pydantic import BaseModel, validator, PositiveFloat
from typing import Optional, Any, List, Dict
from datetime import datetime
from uuid import UUID

# schema


class courier_Status(BaseModel):
    id: UUID
    status: bool


class CourierFilterRequest(BaseModel):
    search: Optional[str] = None
    courier: Optional[str] = None
    mode: Optional[str] = None
    weight: Optional[str] = None


class GetSingleContract(BaseModel):
    id: int


class CourierDataModel(BaseModel):
    carrier_title: str
    token: str


class RateFields(BaseModel):
    percentage_rate: Optional[float] = None
    absolute_rate: Optional[float] = None
    base_rate_zone_a: Optional[float] = None
    base_rate_zone_b: Optional[float] = None
    base_rate_zone_c: Optional[float] = None
    base_rate_zone_d: Optional[float] = None
    base_rate_zone_e: Optional[float] = None
    additional_rate_zone_a: Optional[float] = None
    additional_rate_zone_b: Optional[float] = None
    additional_rate_zone_c: Optional[float] = None
    additional_rate_zone_d: Optional[float] = None
    additional_rate_zone_e: Optional[float] = None
    rto_base_rate_zone_a: Optional[float] = None
    rto_base_rate_zone_b: Optional[float] = None
    rto_base_rate_zone_c: Optional[float] = None
    rto_base_rate_zone_d: Optional[float] = None
    rto_base_rate_zone_e: Optional[float] = None
    rto_additional_rate_zone_a: Optional[float] = None
    rto_additional_rate_zone_b: Optional[float] = None
    rto_additional_rate_zone_c: Optional[float] = None
    rto_additional_rate_zone_d: Optional[float] = None
    rto_additional_rate_zone_e: Optional[float] = None
    rate_type: Optional[str] = "forward"
    isActive: Optional[bool] = False
    client_id: Optional[int] = None
    company_id: Optional[int] = 1
    company_contract_id: Optional[int] = None
    aggregator_courier_id: Optional[int] = None


class CourierAssignRequest(BaseModel):
    id: int
    data: Optional[Dict[str, Any]] = None  # Accepts object (dict) instead of list
    courier_slug: str
    is_rate_add: bool = False  #  changed this
    rates: Optional[RateFields] = None  #  added this


class SingleRateUploadModel(BaseModel):
    forward_a: PositiveFloat
    forward_b: PositiveFloat
    forward_c: PositiveFloat
    forward_d: PositiveFloat
    forward_e: PositiveFloat
    cod_rate: PositiveFloat
    cod_percentage: PositiveFloat
    rto_a: PositiveFloat
    rto_b: PositiveFloat
    rto_c: PositiveFloat
    rto_d: PositiveFloat
    rto_e: PositiveFloat
    courier_name: str
    contract_id: int


# ============================================
# COURIER SETTINGS - PINCODE BLOCKING
# ============================================

class BlockedPincodeRequest(BaseModel):
    courier_uuid: UUID
    pincodes: List[str]
    reason: Optional[str] = None


class RemoveBlockedPincodeRequest(BaseModel):
    courier_uuid: UUID
    pincodes: List[str]


class GetBlockedPincodesRequest(BaseModel):
    courier_uuid: UUID


class BlockedPincodeResponse(BaseModel):
    pincode: str
    reason: Optional[str] = None
    created_at: Optional[str] = None