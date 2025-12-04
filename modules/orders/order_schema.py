import re
from enum import Enum
from uuid import UUID
from typing import Optional, Any, List, Union
from datetime import datetime, timedelta

from pytz import timezone as pytz_timezone
from pydantic import BaseModel, field_validator, model_validator

# schema
from schema.base import DBBaseModel

# utils
from utils.string import clean_text, clean_phone


class PaymentMode(str, Enum):
    """Valid payment modes"""

    COD = "COD"
    PREPAID = "prepaid"


# ============================================
# REGEX PATTERNS
# ============================================

# Name: letters, numbers, spaces, period, apostrophe, hyphen
NAME_REGEX = re.compile(r"^[a-zA-Z0-9\s.'\-]+$")

# Address: letters, numbers, spaces, common punctuation
ADDRESS_REGEX = re.compile(r"^[a-zA-Z0-9\s,.\-#&;()\/]+$")

# Phone: 10 digits starting with 6-9
PHONE_REGEX = re.compile(r"^[6-9]\d{9}$")

# Pincode: exactly 6 digits
PINCODE_REGEX = re.compile(r"^\d{6}$")

# Email
EMAIL_REGEX = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")

# GSTIN: 22AAAAA0000A1Z5
GSTIN_REGEX = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$")

# Order ID: letters, numbers, dash, underscore, dot
ORDER_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-\.]+$")

# Eway bill: 12 digits
EWAY_BILL_REGEX = re.compile(r"^\d{12}$")


# ============================================
# CONSTANTS
# ============================================

WEIGHT_MIN = 0.1  # kg
WEIGHT_MAX = 100  # kg
DIMENSION_MIN = 0.5  # cm
DIMENSION_MAX = 300  # cm
VOLUMETRIC_WEIGHT_MAX = 100  # kg
EWAY_BILL_THRESHOLD = 50000  # INR


# ============================================
# HELPER FUNCTIONS
# ============================================


def calculate_volumetric_weight(length: float, breadth: float, height: float) -> float:
    """Calculate volumetric weight from dimensions."""
    return round((length * breadth * height) / 5000, 3)


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
        v = clean_text(v, 100)
        if not v:
            raise ValueError("Order ID is required")
        if len(v) > 100:
            raise ValueError("Order ID cannot exceed 100 characters")
        if not ORDER_ID_REGEX.match(v):
            raise ValueError(
                "Order ID can only contain letters, numbers, dash, underscore, and dot"
            )
        return v

    @field_validator("channel")
    @classmethod
    def validate_channel(cls, v):
        if v:
            return clean_text(v, 100)
        return "custom"

    @field_validator("order_date", mode="before")
    @classmethod
    def validate_order_date(cls, v):
        """Validate order date is within ±7 days of current date"""
        IST = pytz_timezone("Asia/Kolkata")
        now = datetime.now(IST)

        # Parse the date if it's a string
        if isinstance(v, str):
            v = v.strip()
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

            if parsed_date.tzinfo is None:
                parsed_date = IST.localize(parsed_date)
            v = parsed_date

        if isinstance(v, datetime):
            if v.tzinfo is None:
                v = IST.localize(v)

            min_date = now - timedelta(days=7)
            max_date = now + timedelta(days=7)

            v_date = v.date()
            min_date_only = min_date.date()
            max_date_only = max_date.date()

            if v_date < min_date_only or v_date > max_date_only:
                raise ValueError(
                    "Order date must be within 7 days of today (past or future)"
                )

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

    @field_validator("consignee_full_name")
    @classmethod
    def validate_name(cls, v):
        v = clean_text(v, 100)
        if not v:
            raise ValueError("Consignee name is required")
        if len(v) < 2:
            raise ValueError("Name must be at least 2 characters")
        if not NAME_REGEX.match(v):
            raise ValueError("Name contains invalid special characters")
        return v

    @field_validator("consignee_phone", mode="before")
    @classmethod
    def validate_phone(cls, v):
        v = clean_phone(v)
        if not v:
            raise ValueError("Phone number is required")
        if not PHONE_REGEX.match(v):
            raise ValueError(
                "Phone number must be 10 digits starting with 6, 7, 8, or 9"
            )
        return v

    @field_validator("consignee_alternate_phone", mode="before")
    @classmethod
    def validate_alt_phone(cls, v):
        if not v:
            return None
        v = clean_phone(v)
        if v and not PHONE_REGEX.match(v):
            raise ValueError(
                "Alternate phone must be 10 digits starting with 6, 7, 8, or 9"
            )
        return v

    @field_validator("consignee_email")
    @classmethod
    def validate_email(cls, v):
        if not v:
            return None
        v = clean_text(v, 150).lower()
        if v and not EMAIL_REGEX.match(v):
            raise ValueError("Invalid email format")
        return v

    @field_validator("consignee_company")
    @classmethod
    def validate_company(cls, v):
        if not v:
            return None
        return clean_text(v, 100)

    @field_validator("consignee_gstin")
    @classmethod
    def validate_gstin(cls, v):
        if not v:
            return None
        v = clean_text(v, 15).upper()
        if v and not GSTIN_REGEX.match(v):
            raise ValueError("Invalid GSTIN format (e.g., 22AAAAA0000A1Z5)")
        return v

    @field_validator("consignee_address")
    @classmethod
    def validate_address(cls, v):
        v = clean_text(v, 255)
        if not v:
            raise ValueError("Address is required")
        if len(v) < 10:
            raise ValueError("Address must be at least 10 characters")
        if not ADDRESS_REGEX.match(v):
            raise ValueError("Address contains invalid special characters")
        return v

    @field_validator("consignee_landmark")
    @classmethod
    def validate_landmark(cls, v):
        if not v:
            return None
        v = clean_text(v, 200)
        if v and not ADDRESS_REGEX.match(v):
            raise ValueError("Landmark contains invalid special characters")
        return v

    @field_validator("consignee_pincode", mode="before")
    @classmethod
    def validate_pincode(cls, v):
        v = str(v).strip()
        if not PINCODE_REGEX.match(v):
            raise ValueError("Pincode must be exactly 6 digits")
        return v

    @field_validator("consignee_city")
    @classmethod
    def validate_city(cls, v):
        v = clean_text(v, 150)
        if not v:
            raise ValueError("City is required")
        return v

    @field_validator("consignee_state")
    @classmethod
    def validate_state(cls, v):
        v = clean_text(v, 100)
        if not v:
            raise ValueError("State is required")
        return v

    @field_validator("consignee_country")
    @classmethod
    def validate_country(cls, v):
        if not v:
            return "India"
        return clean_text(v, 100)


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

    @model_validator(mode="after")
    def validate_billing_fields(self):
        """Validate billing fields when billing is different from consignee"""
        if not self.is_billing_same_as_consignee:
            # Validate required fields
            if not self.billing_full_name or len(self.billing_full_name.strip()) < 2:
                raise ValueError(
                    "Billing name is required and must be at least 2 characters"
                )
            if not self.billing_phone:
                raise ValueError("Billing phone is required")
            if not self.billing_address or len(self.billing_address.strip()) < 10:
                raise ValueError(
                    "Billing address is required and must be at least 10 characters"
                )
            if not self.billing_pincode:
                raise ValueError("Billing pincode is required")
            if not self.billing_city:
                raise ValueError("Billing city is required")
            if not self.billing_state:
                raise ValueError("Billing state is required")

            # Validate formats
            if self.billing_full_name and not NAME_REGEX.match(
                self.billing_full_name.strip()
            ):
                raise ValueError("Billing name contains invalid special characters")

            phone = clean_phone(self.billing_phone)
            if phone and not PHONE_REGEX.match(phone):
                raise ValueError(
                    "Billing phone must be 10 digits starting with 6, 7, 8, or 9"
                )
            self.billing_phone = phone

            if self.billing_address and not ADDRESS_REGEX.match(
                self.billing_address.strip()
            ):
                raise ValueError("Billing address contains invalid special characters")

            if self.billing_pincode and not PINCODE_REGEX.match(
                str(self.billing_pincode).strip()
            ):
                raise ValueError("Billing pincode must be exactly 6 digits")

            if self.billing_email:
                email = clean_text(self.billing_email, 150).lower()
                if email and not EMAIL_REGEX.match(email):
                    raise ValueError("Invalid billing email format")
                self.billing_email = email

        return self


class PickupDetailsInput(BaseModel):
    """Pickup location selection"""

    pickup_location_code: str

    @field_validator("pickup_location_code")
    @classmethod
    def validate_pickup_code(cls, v):
        v = clean_text(v, 255)
        if not v:
            raise ValueError("Pickup location code is required")
        return v


class PaymentDetailsInput(BaseModel):
    """Payment and charges"""

    payment_mode: str
    shipping_charges: Optional[float] = 0
    cod_charges: Optional[float] = 0
    discount: Optional[float] = 0
    gift_wrap_charges: Optional[float] = 0
    other_charges: Optional[float] = 0
    tax_amount: Optional[float] = 0
    eway_bill_number: Optional[str] = None

    # Calculated server-side
    total_amount: Optional[float] = None
    order_value: Optional[float] = None
    cod_to_collect: Optional[float] = None

    @field_validator("payment_mode")
    @classmethod
    def validate_payment_mode(cls, v):
        v = str(v).strip()
        # Normalize to standard format
        if v.lower() == "cod":
            return PaymentMode.COD.value
        elif v.lower() == "prepaid":
            return PaymentMode.PREPAID.value
        else:
            raise ValueError("Payment mode must be 'COD' or 'prepaid'")

    @field_validator(
        "shipping_charges",
        "cod_charges",
        "gift_wrap_charges",
        "other_charges",
        "tax_amount",
    )
    @classmethod
    def validate_positive_charge(cls, v):
        if v is None:
            return 0
        v = float(v)
        if v < 0:
            raise ValueError("Charges cannot be negative")
        return round(v, 2)

    @field_validator("discount")
    @classmethod
    def validate_discount(cls, v):
        if v is None:
            return 0
        v = float(v)
        if v < 0:
            raise ValueError("Discount cannot be negative")
        return round(v, 2)

    @field_validator("eway_bill_number")
    @classmethod
    def validate_eway_bill(cls, v):
        if not v:
            return None
        v = clean_text(v, 12)
        if v and not EWAY_BILL_REGEX.match(v):
            raise ValueError("E-way bill number must be 12 digits")
        return v


class PackageDetailsInput(BaseModel):
    """Package dimensions and weight"""

    length: float
    breadth: float
    height: float
    weight: float

    @field_validator("length", "breadth", "height")
    @classmethod
    def validate_dimension(cls, v, info):
        v = float(v)
        field_name = info.field_name.capitalize()
        if v < DIMENSION_MIN:
            raise ValueError(f"{field_name} must be at least {DIMENSION_MIN} cm")
        if v > DIMENSION_MAX:
            raise ValueError(f"{field_name} cannot exceed {DIMENSION_MAX} cm")
        return round(v, 3)

    @field_validator("weight")
    @classmethod
    def validate_weight(cls, v):
        v = float(v)
        if v < WEIGHT_MIN:
            raise ValueError(f"Weight must be at least {WEIGHT_MIN} kg")
        if v > WEIGHT_MAX:
            raise ValueError(f"Weight cannot exceed {WEIGHT_MAX} kg")
        return round(v, 3)

    @model_validator(mode="after")
    def validate_volumetric_weight(self):
        """Validate volumetric weight doesn't exceed maximum"""
        vol_weight = calculate_volumetric_weight(self.length, self.breadth, self.height)
        if vol_weight > VOLUMETRIC_WEIGHT_MAX:
            raise ValueError(
                f"Volumetric weight ({vol_weight} kg) cannot exceed {VOLUMETRIC_WEIGHT_MAX} kg"
            )
        return self


class ProductInput(BaseModel):
    """Product line item"""

    name: str
    unit_price: float
    quantity: int
    sku_code: Optional[str] = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        v = clean_text(v, 300)
        if not v:
            raise ValueError("Product name is required")
        return v

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
        if v < 0.1:
            raise ValueError("Unit price must be at least 0.10")
        return round(v, 2)

    @field_validator("sku_code")
    @classmethod
    def validate_sku(cls, v):
        if not v:
            return None
        return clean_text(v, 50)


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

    products: List[ProductInput]

    @field_validator("products")
    @classmethod
    def validate_products(cls, v):
        if not v or len(v) == 0:
            raise ValueError("At least one product is required")
        return v

    @model_validator(mode="after")
    def validate_eway_bill_requirement(self):
        """Validate eway bill is provided for high-value orders"""
        # Calculate total order value
        product_total = sum(p.unit_price * p.quantity for p in self.products)
        extra_charges = (
            (self.shipping_charges or 0)
            + (self.cod_charges or 0)
            + (self.gift_wrap_charges or 0)
            + (self.other_charges or 0)
            + (self.tax_amount or 0)
            - (self.discount or 0)
        )
        total_amount = product_total + extra_charges

        if total_amount >= EWAY_BILL_THRESHOLD and not self.eway_bill_number:
            raise ValueError(
                f"E-way bill number is required for orders ≥ ₹{EWAY_BILL_THRESHOLD:,}"
            )

        return self

    @model_validator(mode="after")
    def validate_discount_limit(self):
        """Validate discount doesn't exceed product total"""
        product_total = sum(p.unit_price * p.quantity for p in self.products)
        if (self.discount or 0) > product_total:
            raise ValueError("Discount cannot exceed total product value")
        return self


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
    pickup_location: Optional[Any] = None

    # Dates
    booking_date: Optional[datetime] = None
    delivered_date: Optional[datetime] = None
    edd: Optional[datetime] = None
    created_at: datetime

    # Items
    items: Optional[List[OrderItemResponse]] = None

    # Repeat customer
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
    eway_bill_number: Optional[str] = None

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


class customerResponseModel(BaseModel):
    """Customer details response"""

    consignee_full_name: str
    consignee_phone: str
    consignee_email: Optional[str] = None
    consignee_alternate_phone: Optional[str] = None
    consignee_company: Optional[str] = None
    consignee_gstin: Optional[str] = None
    consignee_address: str
    consignee_landmark: Optional[str] = None
    consignee_pincode: str
    consignee_city: str
    consignee_state: str
    consignee_country: str
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


# ============================================
# LEGACY COMPATIBILITY - Order_Model
# Required by: shipping partners, notifications, documents, etc.
# ============================================


class Order_Base_Model(BaseModel):
    """
    Base order model with all order fields.
    Used by shipping partners and other services.
    """

    # Order identification
    order_id: str
    order_date: Union[datetime, str]
    channel: Optional[str] = "Custom"

    # Consignee details
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

    # Pickup
    pickup_location_code: str

    # Payment
    payment_mode: str
    shipping_charges: Optional[float] = 0
    cod_charges: Optional[float] = 0
    discount: Optional[float] = 0
    gift_wrap_charges: Optional[float] = 0
    other_charges: Optional[float] = 0
    tax_amount: Optional[float] = 0
    eway_bill_number: Optional[str] = None
    total_amount: Optional[float] = None
    order_value: Optional[float] = None
    cod_to_collect: Optional[float] = 0

    # Package
    length: float
    breadth: float
    height: float
    weight: float

    # Shipment fields
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
    """
    Full order model for shipping partners and services.
    """

    aggregator: Optional[str] = None
    zone: Optional[str] = None
    client_id: int = 0

    courier_status: Optional[str] = None
    shipping_partner_order_id: Optional[str] = None
    shipping_partner_shipping_id: Optional[str] = None

    pickup_location: Optional[Any] = None

    cod_remittance_cycle_id: Optional[int] = None

    clone_order_count: int = 0
    cancel_count: int = 0
