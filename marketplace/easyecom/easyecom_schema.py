from enum import Enum
from uuid import UUID
from typing import Optional, Union, List

from pydantic import BaseModel, validator, Json

# from typing import Union

from fastapi import HTTPException
from schema.base import DBBaseModel


# AuthInsertModel schema parent
class AuthInsertModel(BaseModel):
    username: str
    password: str
    token: Optional[str] = ""
    account_no: Optional[str] = ""
    service_type: Optional[str] = ""
    eeApiToken: str

    # username validations
    @validator("username")
    def name_must_contain_space(cls, v):
        # remove space
        if not v.strip():
            raise ValueError("username is required")
        return v.strip()

    # password validations
    @validator("password")
    def password_must_contain_space(cls, v):
        # remove space
        if not v.strip():
            raise ValueError("password is required")
        return v.strip()

    # eeApiToken validations
    @validator("eeApiToken")
    def eeApiToken_must_contain_space(cls, v):
        # remove space
        if not v.strip():
            raise ValueError("eeApiToken is required")
        return v.strip()


class breakup_types(BaseModel):
    Item_Amount_Excluding_Tax: float
    Item_Amount_IGST: float


class custom_fields(BaseModel):
    pass


class order_items(BaseModel):
    suborder_id: int
    suborder_num: str
    invoicecode: Optional[str] = None
    item_collectable_amount: float
    shipment_type: str
    suborder_quantity: float
    item_quantity: int
    returned_quantity: int
    cancelled_quantity: int
    shipped_quantity: int
    tax_type: str
    product_id: int
    company_product_id: int
    sku: str
    expiry_type: int
    sku_type: str
    sub_product_count: int
    marketplace_sku: str
    listing_ref_number: str
    listing_id: str
    productName: str
    description: Optional[str] = None
    category: str
    brand: str
    brand_id: int
    model_no: str
    product_tax_code: Optional[str] = None
    ean: str
    size: str
    cost: float
    mrp: float
    weight: float
    length: float
    width: float
    height: float
    scheme_applied: int
    custom_fields: List = []
    serials: List = []
    tax_rate: int
    selling_price: str
    breakup_types: breakup_types
    station_scanned_quantity: int
    batch_scanned_quantity: int
    assigned_quantity: int


class credentials(BaseModel):
    username: str
    password: str
    token: str
    account_no: str
    service_type: str
    eeApiToken: str


class ShippingObjectModel(BaseModel):
    invoice_id: int
    order_id: int
    reference_code: str
    company_name: str
    warehouse_id: int
    seller_gst: str
    assigned_company_name: str
    assigned_warehouse_id: int
    assigned_company_gst: str
    warehouse_contact: str
    pickup_address: str
    pickup_city: str
    pickup_state: str
    pickup_state_code: str
    pickup_pin_code: str
    pickup_country: str
    invoice_currency_code: str
    order_type: str
    order_type_key: str
    replacement_order: int
    marketplace: str
    MarketCId: int
    marketplace_id: int
    market_shipped: int
    merchant_c_id: int
    qcPassed: int
    salesmanUserId: int
    order_date: str
    tat: str
    available_after: Optional[str] = None
    invoice_date: str
    import_date: str
    last_update_date: str
    manifest_date: Optional[str] = None
    manifest_no: Optional[str] = None
    invoice_number: Optional[str] = None
    marketplace_invoice_num: str
    shipping_last_update_date: Optional[str] = None
    batch_id: int
    batch_created_at: str
    message: Optional[str] = None
    courier_aggregator_name: Optional[str] = None
    courier: str
    carrier_id: int
    awb_number: Optional[str] = None
    Package_Weight: int
    Package_Height: int
    Package_Length: int
    Package_Width: int
    order_status: str
    order_status_id: int
    easyecom_order_history: Optional[str] = None
    shipping_status: Optional[str] = None
    shipping_status_id: Optional[str] = None
    tracking_url: Optional[str] = None
    shipping_history: Optional[str] = None
    payment_mode: str
    payment_mode_id: int
    payment_gateway_transaction_number: Optional[str] = None
    buyer_gst: str
    customer_name: str
    shipping_name: str
    contact_num: str
    address_line_1: str
    address_line_2: Optional[str] = None
    city: str
    pin_code: str
    state: str
    state_code: str
    country: str
    country_code: int
    email: str
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    billing_name: str
    billing_address_1: str
    billing_address_2: Optional[str] = None
    billing_city: str
    billing_state: str
    billing_state_code: str
    billing_pin_code: str
    billing_country: str
    billing_mobile: str
    order_quantity: int
    documents: Optional[str] = None
    invoice_documents: Optional[str] = None
    collectable_amount: float
    total_amount: float
    total_tax: float
    breakup_types: breakup_types
    tcs_rate: float
    tcs_amount: float
    customer_code: str
    order_items: List[order_items]


# ShippingInsertModel schema parent
class ShippingInsertModel(BaseModel):
    order_data: ShippingObjectModel
    credentials: credentials


class awb_details(BaseModel):
    awb: str
    courier: str


# CancelShipmentModel schema parent
class CancelShipmentModel(BaseModel):
    awb_details: awb_details
    credentials: credentials


# EasyEcomAccessToken schema parent
class EasyEcomAccessToken(BaseModel):
    email: str
    password: str
    location_key: str


# UpdateTrackingStatus schema parent
class UpdateTrackingStatus(BaseModel):
    current_shipment_status_id: int
    awb: str
    estimated_delivery_date: str
    delivery_date: str


class Auth_Model(
    AuthInsertModel,
    DBBaseModel,
):
    pass
