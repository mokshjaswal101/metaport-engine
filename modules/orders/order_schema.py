"""
Order Pydantic Schemas

Updated for new normalized database structure:
- Removed: order_type, tracking_id, source, marketplace_order_id, order_tags
- Removed: tracking_info, action_history, product_quantity (now computed/separate tables)
- Renamed: billing_is_same_as_consignee → is_billing_same_as_consignee
- Added: cod_to_collect
"""

import re
from enum import Enum
from uuid import UUID
from typing import Optional, Any, List, Dict, Union
from datetime import datetime, timedelta

from pytz import timezone as pytz_timezone
from pydantic import BaseModel, field_validator, model_validator

# schema
from schema.base import DBBaseModel
from modules.pickup_location.pickup_location_schema import (
    PickupLocationModel,
    PickupLocationResponseModel,
)


# ============================================
# COMPONENT SCHEMAS
# ============================================

class OrderDetailsInput(BaseModel):
    """Order identification fields"""
    order_id: str
    order_date: Union[datetime, str]
    channel: Optional[str] = "Custom"
    
    @field_validator("order_id")
    @classmethod
    def validate_order_id(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("Order ID is required")
        if len(v) > 100:
            raise ValueError("Order ID cannot exceed 100 characters")
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', v):
            raise ValueError("Order ID can only contain letters, numbers, dash, underscore, and dot")
        return v
    
    @field_validator("order_date", mode="before")
    @classmethod
    def validate_order_date(cls, v):
        """Validate order date is within ±7 days of current date"""
        IST = pytz_timezone("Asia/Kolkata")
        now = datetime.now(IST)
        
        # Parse the date if it's a string
        if isinstance(v, str):
            v = v.strip()
            # Try various formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y",
            ]
            parsed_date = None
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(v, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_date is None:
                raise ValueError("Invalid date format. Use YYYY-MM-DD or DD-MM-YYYY")
            
            # Localize to IST if naive
            if parsed_date.tzinfo is None:
                parsed_date = IST.localize(parsed_date)
            v = parsed_date
        
        # If datetime, ensure it has timezone
        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = IST.localize(v)
            
            # Check if within ±7 days
            min_date = now - timedelta(days=7)
            max_date = now + timedelta(days=7)
            
            # Compare dates only (ignore time)
            v_date = v.date()
            min_date_only = min_date.date()
            max_date_only = max_date.date()
            
            if v_date < min_date_only or v_date > max_date_only:
                raise ValueError("Order date must be within 7 days of today (past or future)")
        
        return v


class ConsigneeDetailsInput(BaseModel):
    """Consignee (delivery) address fields"""
    consignee_full_name: str
    consignee_phone: str
    consignee_email: Optional[str] = None
    consignee_alternate_phone: Optional[str] = None
    consignee_company: Optional[str] = None
    consignee_gstin: Optional[str] = None
    consignee_address: str
    consignee_landmark: Optional[str] = None
    consignee_pincode: Union[str, int]
    consignee_city: str
    consignee_state: str
    consignee_country: Optional[str] = "India"
    
    @field_validator("consignee_phone", "consignee_alternate_phone", mode="before")
    @classmethod
    def clean_phone(cls, v):
        if not v:
            return v
        v = str(v).strip()
        if v.startswith("+91"):
            v = v[3:]
        elif v.startswith("91") and len(v) > 10:
            v = v[2:]
        return v
    
    @field_validator("consignee_pincode", mode="before")
    @classmethod
    def validate_pincode(cls, v):
        v = str(v).strip()
        if not re.match(r'^\d{6}$', v):
            raise ValueError("Pincode must be exactly 6 digits")
        return v


class BillingDetailsInput(BaseModel):
    """Billing address fields (when different from consignee)"""
    is_billing_same_as_consignee: Optional[bool] = True
    billing_full_name: Optional[str] = None
    billing_phone: Optional[str] = None
    billing_email: Optional[str] = None
    billing_address: Optional[str] = None
    billing_landmark: Optional[str] = None
    billing_pincode: Optional[str] = None
    billing_city: Optional[str] = None
    billing_state: Optional[str] = None
    billing_country: Optional[str] = None
    
    # Backward compatibility alias
    @model_validator(mode="before")
    @classmethod
    def handle_legacy_field(cls, values):
        if isinstance(values, dict):
            # Handle legacy field name
            if "billing_is_same_as_consignee" in values and "is_billing_same_as_consignee" not in values:
                values["is_billing_same_as_consignee"] = values.pop("billing_is_same_as_consignee")
        return values


class PickupDetailsInput(BaseModel):
    """Pickup location selection"""
    pickup_location_code: str


class PaymentDetailsInput(BaseModel):
    """Payment and charges"""
    payment_mode: str
    shipping_charges: Optional[float] = 0
    cod_charges: Optional[float] = 0
    discount: Optional[float] = 0
    gift_wrap_charges: Optional[float] = 0
    other_charges: Optional[float] = 0
    tax_amount: Optional[float] = 0
    tax_percentage: Optional[float] = 0  # Used for calculation if provided
    
    # These are calculated server-side but can be provided
    total_amount: Optional[float] = None
    order_value: Optional[float] = None
    cod_to_collect: Optional[float] = None
    
    @field_validator("payment_mode")
    @classmethod
    def validate_payment_mode(cls, v):
        v = str(v).strip().lower()
        if v not in ["cod", "prepaid"]:
            raise ValueError("Payment mode must be 'cod' or 'prepaid'")
        return v


class PackageDetailsInput(BaseModel):
    """Package dimensions and weight"""
    length: float
    breadth: float
    height: float
    weight: float
    
    @field_validator("length", "breadth", "height")
    @classmethod
    def validate_dimension(cls, v):
        v = float(v)
        if v < 0.1:
            raise ValueError("Dimension must be at least 0.1 cm")
        if v > 300:
            raise ValueError("Dimension cannot exceed 300 cm")
        return round(v, 3)
    
    @field_validator("weight")
    @classmethod
    def validate_weight(cls, v):
        v = float(v)
        if v < 0.001:
            raise ValueError("Weight must be at least 0.001 kg")
        if v > 100:
            raise ValueError("Weight cannot exceed 100 kg")
        return round(v, 3)


class ProductInput(BaseModel):
    """Product line item"""
    name: str
    unit_price: float
    quantity: int
    sku_code: Optional[str] = None
    
    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        v = str(v).strip()
        if not v:
            raise ValueError("Product name is required")
        return v[:255]
    
    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, v):
        v = int(v)
        if v < 1:
            raise ValueError("Quantity must be at least 1")
        return v
    
    @field_validator("unit_price")
    @classmethod
    def validate_price(cls, v):
        v = float(v)
        if v < 0:
            raise ValueError("Unit price cannot be negative")
        return round(v, 2)


# ============================================
# REQUEST SCHEMAS
# ============================================

class Order_create_request_model(
    OrderDetailsInput,
    ConsigneeDetailsInput,
    BillingDetailsInput,
    PickupDetailsInput,
    PaymentDetailsInput,
    PackageDetailsInput,
):
    """
    Complete order creation request.
    
    All fields from component schemas are included.
    Products is a list of ProductInput.
    Courier is optional - if provided, will attempt immediate AWB assignment.
    """
    products: List[ProductInput]
    courier: Optional[int] = None  # Contract ID for immediate AWB assignment


# ============================================
# RESPONSE SCHEMAS
# ============================================

class ProductResponse(BaseModel):
    """Product in response"""
    id: int
    name: str
    sku_code: Optional[str] = None
    quantity: int
    unit_price: float


class OrderItemResponse(BaseModel):
    """Order item (product) response"""
    id: int
    name: str
    sku_code: Optional[str] = None
    quantity: int
    unit_price: float
    
    class Config:
        from_attributes = True


class TrackingEventResponse(BaseModel):
    """Tracking event response"""
    id: int
    status: str
    description: Optional[str] = None
    sub_info: Optional[str] = None
    location: Optional[str] = None
    courier_status: Optional[str] = None
    event_datetime: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class AuditLogResponse(BaseModel):
    """Audit log response"""
    id: int
    action: str
    message: str
    user_name: Optional[str] = None
    timestamp: datetime
    
    class Config:
        from_attributes = True


class BillingResponse(BaseModel):
    """Billing/freight response"""
    forward_freight: Optional[float] = 0
    forward_cod_charge: Optional[float] = 0
    forward_tax: Optional[float] = 0
    rto_freight: Optional[float] = 0
    rto_tax: Optional[float] = 0
    cod_charge_reversed: Optional[float] = 0
    buy_forward_freight: Optional[float] = 0
    buy_forward_cod_charge: Optional[float] = 0
    buy_forward_tax: Optional[float] = 0
    buy_rto_freight: Optional[float] = 0
    buy_rto_tax: Optional[float] = 0
    
    class Config:
        from_attributes = True


class Order_Response_Model(BaseModel):
    """Standard order response for list views"""
    id: int
    uuid: UUID
    order_id: str
    order_date: datetime
    channel: Optional[str] = None
    client_id: int
    
    # Consignee
    consignee_full_name: str
    consignee_phone: str
    consignee_email: Optional[str] = None
    consignee_address: str
    consignee_pincode: str
    consignee_city: str
    consignee_state: str
    consignee_country: str
    
    # Payment
    payment_mode: str
    total_amount: float
    order_value: float
    cod_to_collect: Optional[float] = 0
    
    # Package
    weight: float
    applicable_weight: float
    volumetric_weight: float
    length: Optional[float] = None
    breadth: Optional[float] = None
    height: Optional[float] = None
    
    # Shipment
    status: str
    sub_status: str
    awb_number: Optional[str] = None
    courier_partner: Optional[str] = None
    zone: Optional[str] = None
    
    # Pickup
    pickup_location_code: str
    pickup_location: Optional[Any] = None  # Can be PickupLocation model or dict
    
    # Dates
    booking_date: Optional[datetime] = None
    delivered_date: Optional[datetime] = None
    edd: Optional[datetime] = None
    created_at: datetime
    
    # Items (from order_item table)
    items: Optional[List[OrderItemResponse]] = None
    
    # For repeat customer indication
    previous_order_count: Optional[int] = 0
    
    class Config:
        from_attributes = True


class Single_Order_Response_Model(Order_Response_Model):
    """Detailed order response for single order view"""
    
    # Additional consignee fields
    consignee_alternate_phone: Optional[str] = None
    consignee_company: Optional[str] = None
    consignee_gstin: Optional[str] = None
    consignee_landmark: Optional[str] = None
    
    # Billing details
    is_billing_same_as_consignee: Optional[bool] = True
    billing_full_name: Optional[str] = None
    billing_phone: Optional[str] = None
    billing_email: Optional[str] = None
    billing_address: Optional[str] = None
    billing_landmark: Optional[str] = None
    billing_pincode: Optional[str] = None
    billing_city: Optional[str] = None
    billing_state: Optional[str] = None
    billing_country: Optional[str] = None
    
    # All charges
    shipping_charges: Optional[float] = 0
    cod_charges: Optional[float] = 0
    discount: Optional[float] = 0
    gift_wrap_charges: Optional[float] = 0
    other_charges: Optional[float] = 0
    tax_amount: Optional[float] = 0
    
    # Package dimensions
    length: float
    breadth: float
    height: float
    
    # Shipment details
    aggregator: Optional[str] = None
    shipment_mode: Optional[str] = None
    shipping_partner_order_id: Optional[str] = None
    shipping_partner_shipping_id: Optional[str] = None
    courier_status: Optional[str] = None
    
    # URLs
    manifest_url: Optional[str] = None
    label_url: Optional[str] = None
    invoice_url: Optional[str] = None
    
    # Status dates
    pickup_completion_date: Optional[datetime] = None
    first_ofp_date: Optional[datetime] = None
    shipped_date: Optional[datetime] = None
    edd: Optional[datetime] = None
    first_ofd_date: Optional[datetime] = None
    rto_initiated_date: Optional[datetime] = None
    rto_delivered_date: Optional[datetime] = None
    last_update_date: Optional[datetime] = None
    
    # Error fields
    shipment_booking_error: Optional[str] = None
    pickup_failed_reason: Optional[str] = None
    rto_reason: Optional[str] = None
    
    # Flags
    is_label_generated: bool = False
    clone_order_count: int = 0
    cancel_count: int = 0
    
    # Related data
    tracking_events: Optional[List[TrackingEventResponse]] = None
    audit_logs: Optional[List[AuditLogResponse]] = None
    billing: Optional[BillingResponse] = None
    
    class Config:
        from_attributes = True


# ============================================
# LEGACY COMPATIBILITY SCHEMAS
# ============================================

# These maintain backward compatibility with existing code

class order_details(BaseModel):
    """Legacy: Order details"""
    order_id: str
    order_date: datetime
    channel: str


class consignee_details(BaseModel):
    """Legacy: Consignee details"""
    consignee_full_name: str
    consignee_phone: str
    consignee_email: Optional[str] = "xyz@gmail.com"
    consignee_alternate_phone: Optional[str] = None
    consignee_company: Optional[str] = ""
    consignee_gstin: Optional[str] = ""
    consignee_address: str
    consignee_landmark: Optional[str] = ""
    consignee_pincode: Union[str, int]
    consignee_city: str
    consignee_state: str
    consignee_country: str


class billing_details(BaseModel):
    """Legacy: Billing details - supports both old and new field names"""
    is_billing_same_as_consignee: Optional[bool] = True
    billing_full_name: Optional[str] = None
    billing_phone: Optional[str] = None
    billing_email: Optional[str] = None
    billing_address: Optional[str] = None
    billing_landmark: Optional[str] = None
    billing_pincode: Optional[str] = None
    billing_city: Optional[str] = None
    billing_state: Optional[str] = None
    billing_country: Optional[str] = None
    
    # Backward compatibility
    @model_validator(mode="before")
    @classmethod
    def handle_legacy_field(cls, values):
        if isinstance(values, dict):
            if "billing_is_same_as_consignee" in values and "is_billing_same_as_consignee" not in values:
                values["is_billing_same_as_consignee"] = values.pop("billing_is_same_as_consignee")
        return values


class pickup_details(BaseModel):
    """Legacy: Pickup details"""
    pickup_location_code: str


class payment_details(BaseModel):
    """Legacy: Payment details"""
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
    """Legacy: Package details"""
    length: float
    breadth: float
    height: float
    weight: float


class product(BaseModel):
    """Legacy: Product"""
    name: str
    unit_price: float
    quantity: int
    sku_code: Optional[str] = ""


class tracking_info_item(BaseModel):
    """Legacy: Tracking info item"""
    status: str
    description: Optional[str] = ""
    subinfo: Optional[str] = ""
    datetime: str
    location: Optional[str] = ""


class Order_Base_Model(Order_create_request_model):
    """Legacy: Base model - kept for compatibility"""
    
    courier_partner: Optional[str] = None
    shipment_mode: Optional[str] = None
    awb_number: Optional[str] = None
    
    applicable_weight: float = 0
    volumetric_weight: float = 0
    
    manifest_url: Optional[str] = None
    label_url: Optional[str] = None
    invoice_url: Optional[str] = None
    
    status: str = "new"
    sub_status: str = "new"
    
    shipment_booking_error: Optional[str] = None
    
    booking_date: Optional[datetime] = None
    pickup_completion_date: Optional[datetime] = None
    first_ofp_date: Optional[datetime] = None
    shipped_date: Optional[datetime] = None
    edd: Optional[datetime] = None
    first_ofd_date: Optional[datetime] = None
    delivered_date: Optional[datetime] = None
    rto_initiated_date: Optional[datetime] = None
    rto_delivered_date: Optional[datetime] = None
    
    pickup_failed_reason: Optional[str] = None
    rto_reason: Optional[str] = None
    
    is_label_generated: bool = False


class Order_Model(Order_Base_Model, DBBaseModel):
    """Legacy: Full order model - kept for compatibility"""
    
    aggregator: Optional[str] = None
    zone: Optional[str] = None
    client_id: int = 0
    
    courier_status: Optional[str] = None
    shipping_partner_order_id: Optional[str] = None
    shipping_partner_shipping_id: Optional[str] = None
    
    pickup_location: Optional[Any] = None  # Can be PickupLocation model or dict
    
    cod_remittance_cycle_id: Optional[int] = None
    
    clone_order_count: int = 0
    cancel_count: int = 0


# ============================================
# FILTER & ACTION SCHEMAS
# ============================================

class Order_filters(BaseModel):
    """Order list filters"""
    batch_size: int
    page_number: int
    order_status: str
    search_term: Optional[str] = None
    start_date: datetime
    end_date: datetime
    date_type: str
    payment_mode: Optional[str] = ""
    courier_filter: Optional[str] = ""
    sku_codes: Optional[str] = ""
    product_name: Optional[str] = ""
    product_quantity: Optional[int] = None
    current_status: Optional[str] = ""
    order_id: Optional[str] = None
    pincode: Optional[str] = ""
    tags: Optional[str] = None
    repeat_customer: Optional[bool] = None
    pickup_location: Optional[str] = None


class Order_Status_Filters(BaseModel):
    """Status count filters"""
    search_term: Optional[str] = None
    start_date: datetime
    end_date: datetime
    date_type: str


class Order_Export_Filters(BaseModel):
    """Export filters"""
    order_status: str
    search_term: Optional[str] = None
    start_date: datetime
    end_date: datetime
    payment_mode: Optional[str] = None
    courier_filter: Optional[str] = None
    sku_codes: Optional[str] = ""
    order_id: Optional[str] = ""


class bulkCancelOrderModel(BaseModel):
    """Bulk cancel request"""
    order_ids: List[str]


class cloneOrderModel(Order_create_request_model):
    """Clone order request"""
    applicable_weight: float
    volumetric_weight: float
    zone: str
    client_id: int


class customerResponseModel(consignee_details, billing_details):
    """Customer details response"""
    pass


class DimensionUpdateModel(BaseModel):
    """Single dimension update"""
    order_id: str
    length: float
    breadth: float
    height: float
    dead_weight: float


class BulkDimensionUpdateModel(BaseModel):
    """Bulk dimension update"""
    bulk_dimensions: List[DimensionUpdateModel]


class UpdatePickupLocationModel(BaseModel):
    """Update pickup location for orders"""
    location_code: str
    order_ids: List[str]


class BulkOrderUploadLogsModel(DBBaseModel):
    """Bulk upload log entry"""
    upload_date: datetime
    order_count: int
    uploaded_order_count: int
    error_order_count: int
    error_file_url: Optional[str] = None
    client_id: int


class BulkImportValidationError(BaseModel):
    """Bulk import validation error"""
    order_id: str
    error_type: str
    error_message: str
    field_name: Optional[str] = None


class BulkImportResponseModel(BaseModel):
    """Bulk import response"""
    total_orders: int
    successful_orders: int
    failed_orders: int


# ============================================
# COD REMITTANCE SCHEMAS
# ============================================

class COD_Remitance_Model(DBBaseModel):
    """COD remittance model"""
    payout_date: datetime
    generated_cod: float
    freight_deduction: float
    early_cod_charges: float
    rto_reversal_amount: float
    remittance_amount: float
    tax_deduction: float
    amount_paid: float
    payment_method: Optional[str] = None
    order_count: int
    utr_number: Optional[str] = None
    remarks: Optional[str] = None
    status: Optional[str] = None
    client_id: int
