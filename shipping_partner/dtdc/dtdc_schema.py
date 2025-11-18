from pydantic import BaseModel, Json
from typing import Optional, List

# schema
from schema.base import DBBaseModel


class Dtdc_origin_details(BaseModel):
    name: str
    phone: str
    alternate_phone: str
    address_line_1: str
    address_line_2: str
    pincode: str
    city: str
    state: str


class Dtdc_destination_details(BaseModel):
    name: str
    phone: str
    alternate_phone: str
    address_line_1: str
    address_line_2: str
    pincode: str
    city: str
    state: str


class Dtdc_return_details(BaseModel):
    address_line_1: str
    address_line_2: str
    city_name: str
    name: str
    phone: str
    pincode: str
    state_name: str
    email: str
    alternate_phone: str


class Dtdc_merge(BaseModel):
    customer_code: str
    service_type_id: str
    load_type: str
    description: str
    dimension_unit: str
    length: float
    width: float
    height: float
    weight_unit: str
    weight: float
    declared_value: float
    num_pieces: str
    origin_details: Dtdc_origin_details
    destination_details: Dtdc_destination_details
    return_details: Dtdc_return_details
    customer_reference_number: str
    cod_collection_mode: str
    cod_amount: str
    commodity_id: str
    eway_bill: str
    is_risk_surcharge_applicable: bool
    invoice_number: str
    invoice_date: str
    reference_number: str


class pieces_detail_msp(BaseModel):
    description: str
    declared_value: str
    weight: float
    height: str
    length: str
    width: str


class Dtdc_mps_merge(Dtdc_merge):
    pieces_detail: List[pieces_detail_msp]


class Dtdc_single_model(BaseModel):
    consignments: List[Dtdc_merge]


class Dtdc_mps_model(BaseModel):
    consignments: List[Dtdc_mps_merge]


class Dtdc_cancel_model(BaseModel):
    AWBNo: List[str]
    customerCode: str


class Dtdc_track_model(BaseModel):
    trkType: str
    strcnno: str
    addtnlDtl: str
