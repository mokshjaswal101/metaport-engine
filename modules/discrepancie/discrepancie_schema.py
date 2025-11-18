from enum import Enum
from uuid import UUID
from typing import Optional, Any, List, Dict
from datetime import datetime

from pydantic import BaseModel, validator

# schema
from schema.base import DBBaseModel


class upload_rate_discrepancie_model(BaseModel):
    awb_number: str
    width: float
    length: float
    height: float
    dead_weight: float
    image1: Optional[str]
    image2: Optional[str]
    image3: Optional[str]

    @validator("awb_number", pre=True)
    def convert_awb_to_string(cls, value):
        return str(value)  # Convert numbers to strings automatically


class Status_Model(BaseModel):
    status: str
    selectedDisStatus: str
    selectedPageNumber: int
    batchSize: int
    startDate: datetime
    endDate: datetime


# Schema for Products under Order
class ProductSchema(BaseModel):
    name: str
    quantity: int
    sku_code: str
    unit_price: float


# Schema for Order with selected fields
class OrderAdminSchema(BaseModel):
    order_id: str
    length: Optional[float] = None
    height: Optional[float] = None
    width: Optional[float] = None
    breadth: Optional[float] = None
    applicable_weight: Optional[float] = None
    volumetric_weight: Optional[float] = None
    client_name: Optional[str] = None  # Extract from 'client' object if present
    products: List[ProductSchema] = []  # Nested Product Details
    courier_partner: Optional[str] = None
    aggregator: Optional[str] = None
    forward_freight: Optional[float] = None
    forward_cod_charge: Optional[float] = None
    forward_tax: Optional[float] = None
    payment_mode: str
    status: str
    rto_freight: Optional[float] = None
    rto_tax: Optional[float] = None


# Schema View History
class view_History_Schema(BaseModel):
    id: int


class OrderClientSchema(BaseModel):
    order_id: str
    length: Optional[float] = None
    height: Optional[float] = None
    width: Optional[float] = None
    breadth: Optional[float] = None
    applicable_weight: Optional[float] = None
    volumetric_weight: Optional[float] = None
    client_name: Optional[str] = None  # Extract from 'client' object if present
    products: List[ProductSchema] = []  # Nested Product Details
    courier_partner: Optional[str] = None
    forward_freight: Optional[float] = None
    forward_cod_charge: Optional[float] = None
    forward_tax: Optional[float] = None
    payment_mode: str
    status: str
    rto_freight: Optional[float] = None
    rto_tax: Optional[float] = None


class HistorySchema(BaseModel):
    awb_number: str
    action_by: str
    status: str
    created_at: datetime


class DisputesSchema(BaseModel):
    width_image: str
    length_image: str
    height_image: str
    label_image: Optional[str] = None
    scale_image: Optional[str] = None
    awb_number: str


class Rate_Discrepancy_Response_Admin_model(BaseModel):
    updated_at: str
    awb_number: str
    client_name: str
    volumetric_weight: Optional[float] = None
    action_by: Optional[str] = None
    length: Optional[float] = None
    height: Optional[float] = None
    width: Optional[float] = None
    applied_weight: Optional[float] = None
    courier_weight: Optional[float] = None
    charged_weight: Optional[float] = None

    charged_weight_charge: Optional[Dict[str, Any]]
    excess_weight_charge: Optional[Dict[str, Any]]
    discrepancie_type: str
    dead_weight: Optional[float] = None
    image1: Optional[str] = None
    image2: Optional[str] = None
    image3: Optional[str] = None
    status: Optional[str] = None
    order: Optional[OrderAdminSchema] = None  # Nested Order Data
    history: List[HistorySchema] = []  # Nested Product Details
    disputes: Optional[List[DisputesSchema]] = None


class Rate_Discrepancy_Response_Client_model(BaseModel):
    updated_at: str
    id: int
    awb_number: str
    client_name: str
    volumetric_weight: Optional[float] = None
    charged_weight: Optional[float] = None
    action_by: Optional[str] = None
    length: Optional[float] = None
    height: Optional[float] = None
    width: Optional[float] = None
    dead_weight: Optional[float] = None
    applied_weight: Optional[float] = None
    courier_weight: Optional[float] = None
    charged_weight: Optional[float] = None
    charged_weight_charge: Optional[Dict[str, Any]]
    excess_weight_charge: Optional[Dict[str, Any]]
    discrepancie_type: str
    image1: Optional[str] = None
    image2: Optional[str] = None
    image3: Optional[str] = None
    status: Optional[str] = None
    order: Optional[OrderClientSchema] = None  # Nested Order Data
    history: List[HistorySchema] = []  # Nested Product Details
    disputes: Optional[List[DisputesSchema]] = None


class History_Schema_Response(BaseModel):
    history: List[HistorySchema] = []  # Nested Product Details


class Report_Discrepancy_Uploaded_Response_model(BaseModel):
    awb_number: str
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    dead_weight: Optional[float] = None
    image1: Optional[str] = None
    image2: Optional[str] = None
    image3: Optional[str] = None


class Report_Discrepancy_Response_model(BaseModel):
    updated_at: str
    awb_number: str
    client_name: str
    volumetric_weight: Optional[float] = None
    charged_weight: Optional[float] = None
    action_by: Optional[str] = None
    length: Optional[float] = None
    height: Optional[float] = None
    width: Optional[float] = None
    dead_weight: Optional[float] = None
    excess_weight: Optional[float] = None
    excess_charge: Optional[Dict[str, Any]] = None
    # order: Optional[OrderSchema] = None  # Nested Order Data
    # history: List[HistorySchema] = []  # Nested Product Details
    # disputes: Optional[List[DisputesSchema]] = None


class Accept_Description_Model(BaseModel):
    awb_number: str


class Accept_Bulk_Description_Model(BaseModel):
    awb_numbers: List[str]


class Dispute_Model(BaseModel):
    product_category: str
    awb_number: str
    product_url: Optional[str] = None
    product_remarks: Optional[str] = None
    height_image: str
    width_image: str
    length_image: str
    scale_image: Optional[str] = None
    label_image: Optional[str] = None


class Bulk_Dispute_Model(BaseModel):
    product_category: str
    awb_numbers: List[str]
    product_url: Optional[str] = None
    product_remarks: Optional[str] = None
    height_image: str
    width_image: str
    length_image: str
    scale_image: Optional[str] = None
    label_image: Optional[str] = None


class Accept_Dispute_Model(BaseModel):
    awb_number: str


class Status_Model_Schema(BaseModel):
    status: str
    action: str
