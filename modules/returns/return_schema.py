from enum import Enum
from uuid import UUID
from typing import Optional, Any, List, Dict
from datetime import datetime

from pydantic import BaseModel

# schema
from schema.base import DBBaseModel
from modules.pickup_location.pickup_location_schema import (
    PickupLocationModel,
    PickupLocationResponseModel,
)


class order_details(BaseModel):
    order_id: str
    order_date: datetime
    channel: str


class consignee_details(BaseModel):
    consignee_full_name: str
    consignee_phone: str
    consignee_email: Optional[str]
    consignee_alternate_phone: Optional[str]
    consignee_company: Optional[str] = ""
    consignee_gstin: Optional[str] = ""
    consignee_address: str
    consignee_landmark: Optional[str] = ""
    consignee_pincode: str
    consignee_city: str
    consignee_state: str
    consignee_country: str


class billing_details(BaseModel):
    billing_is_same_as_consignee: bool
    billing_full_name: Optional[str]
    billing_phone: Optional[str]
    billing_email: Optional[str]
    billing_address: Optional[str]
    billing_landmark: Optional[str]
    billing_pincode: Optional[str]
    billing_city: Optional[str]
    billing_state: Optional[str]
    billing_country: Optional[str]


class pickup_details(BaseModel):
    pickup_location_code: str


class payment_details(BaseModel):
    payment_mode: str
    shipping_charges: Optional[float] = 0
    cod_charges: Optional[float] = 0
    discount: Optional[float] = 0
    gift_wrap_charges: Optional[float] = 0
    other_charges: Optional[float] = 0
    total_amount: float
    order_value: float
    tax_amount: Optional[float] = 0


class package_details(BaseModel):
    length: float
    breadth: float
    height: float
    weight: float


class product(BaseModel):
    name: str
    unit_price: float
    quantity: int
    sku_code: Optional[str] = ""


class tracking_info_item(BaseModel):
    status: str
    description: Optional[str] = ""
    subinfo: Optional[str] = ""
    datetime: str
    location: Optional[str] = ""


class Order_create_request_model(
    order_details,
    consignee_details,
    billing_details,
    pickup_details,
    payment_details,
    package_details,
):
    products: List[product]
    qc_reason: Optional[str] = None


class Order_Base_Model(Order_create_request_model):

    order_type: str

    courier_partner: Optional[str]
    shipment_mode: Optional[str]
    awb_number: Optional[str]

    applicable_weight: float
    volumetric_weight: float

    manifest_url: Optional[str]
    label_url: Optional[str]
    invoice_url: Optional[str]
    tracking_id: Optional[str]

    status: str
    sub_status: str


class Order_Model(Order_Base_Model, DBBaseModel):

    aggregator: Optional[str]

    tracking_info: Optional[List[tracking_info_item]] = []

    zone: Optional[str]

    action_history: Optional[List[Dict[str, Any]]] = []

    company_id: int
    client_id: int

    courier_status: Optional[str]
    shipping_partner_order_id: Optional[str]
    shipping_partner_shipping_id: Optional[str]

    pickup_location: Optional[PickupLocationModel]

    forward_freight: Optional[float]
    forward_cod_charge: Optional[float]
    forward_tax: Optional[float]

    rto_freight: Optional[float]
    rto_tax: Optional[float]

    cod_remittance_cycle_id: Optional[int]

    source: Optional[str]
    marketplace_order_id: Optional[str]

    invoice_id: Optional[int] = None


class Order_Response_Model(Order_Base_Model):

    pickup_location: Optional[PickupLocationResponseModel]

    forward_freight: Optional[float]
    forward_cod_charge: Optional[float]
    forward_tax: Optional[float]

    rto_freight: Optional[float]
    rto_tax: Optional[float]


class Single_Order_Response_Model(Order_Base_Model):
    pickup_location: Optional[PickupLocationResponseModel]
    tracking_info: Optional[List[tracking_info_item]]
    action_history: Optional[List[Dict[str, Any]]]


class Order_filters(BaseModel):
    batch_size: int
    page_number: int
    order_status: str
    search_term: Optional[str]
    start_date: datetime
    end_date: datetime


class Get_Order_Usging_AWB_OR_Order_Id(BaseModel):
    items: str
    type: str


class Order_Status_Filters(BaseModel):
    search_term: Optional[str]
    start_date: datetime
    end_date: datetime


class Order_Export_Filters(BaseModel):
    order_status: str
    search_term: Optional[str]
    start_date: datetime
    end_date: datetime


class COD_Remitance_Model(DBBaseModel):
    payout_date: datetime
    generated_cod: float

    freight_deduction: float
    early_cod_charges: float
    rto_reversal_amount: float
    remittance_amount: float

    tax_deduction: float
    amount_paid: float

    payment_method: Optional[str]

    order_count: int

    utr_number: Optional[str]

    remarks: Optional[str]
    status: Optional[str]

    client_id: int


class bulkCancelOrderModel(BaseModel):
    order_ids: List[str]


# DEV RETURN ORDER SCHEMAS - Field mappings for return orders
class dev_return_pickup_details(BaseModel):
    """Pickup details for return orders - maps consignee fields to pickup fields"""

    pickup_full_name: str
    pickup_phone: str
    pickup_email: Optional[str]
    pickup_alternate_phone: Optional[str]
    pickup_company: Optional[str] = ""
    pickup_gstin: Optional[str] = ""
    pickup_address: str
    pickup_landmark: Optional[str] = ""
    pickup_pincode: str
    pickup_city: str
    pickup_state: str
    pickup_country: str


class dev_return_location_details(BaseModel):
    """Return location details - maps pickup_location_code to return_location_code"""

    pickup_location_code: str


class dev_return_payment_details(BaseModel):
    """Payment details for return orders - always prepaid"""

    payment_mode: str = "prepaid"  # Always prepaid for returns
    shipping_charges: Optional[float] = 0
    cod_charges: Optional[float] = 0
    discount: Optional[float] = 0
    gift_wrap_charges: Optional[float] = 0
    other_charges: Optional[float] = 0
    total_amount: float
    order_value: float
    tax_amount: Optional[float] = 0


class Dev_Return_Order_Create_Request_Model(
    order_details,
    dev_return_pickup_details,
    dev_return_location_details,
    dev_return_payment_details,
    package_details,
):
    """Schema for dev return order creation with mapped field names"""

    products: List[product]
    return_reason: Optional[str] = None
    courier: int
