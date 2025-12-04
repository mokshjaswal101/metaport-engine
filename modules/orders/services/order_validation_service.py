"""
Order Validation Service

Centralized validation for all order-related operations.
Used by both single order creation and bulk import.

Validation Categories:
1. Format validation (phone, email, pincode, GSTIN)
2. Business rule validation (serviceability, limits)
3. Data integrity validation (order exists, pickup location)

FIXED ISSUES:
- Memory leak in pincode cache (now uses Python's built-in lru_cache)
- Billing address validation now includes email and GSTIN validation
- Better pincode error messages (distinguishes format vs serviceability)
- Centralized phone number normalization
- Thread-safe pincode caching with TTLCache
- Improved phone normalization (handles spaces, dashes)
- Unicode normalization for data sanitization
"""

import re
import unicodedata
from functools import lru_cache
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from threading import Lock
from sqlalchemy.orm import Session

from cachetools import TTLCache

from models import Order, Pickup_Location, Pincode_Mapping
from logger import logger


# Thread-safe cache for pincode lookups
_pincode_cache = TTLCache(maxsize=1000, ttl=3600)  # 1 hour TTL
_cache_lock = Lock()

# Cache size limits (kept for backward compatibility)
PINCODE_CACHE_SIZE = 1000
PICKUP_LOCATION_CACHE_SIZE = 100


@dataclass
class ValidationError:
    """Represents a single validation error"""

    field: str
    message: str
    code: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validation operation"""

    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, field: str, message: str, code: str, value: Any = None):
        """Add a validation error"""
        self.errors.append(ValidationError(field, message, code, value))
        self.is_valid = False

    def add_warning(self, message: str):
        """Add a validation warning (non-blocking)"""
        self.warnings.append(message)

    def merge(self, other: "ValidationResult"):
        """Merge another validation result into this one"""
        if not other.is_valid:
            self.is_valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)

    def to_dict(self) -> Dict:
        """Convert to dictionary for API response"""
        return {
            "is_valid": self.is_valid,
            "errors": [
                {"field": e.field, "message": e.message, "code": e.code}
                for e in self.errors
            ],
            "warnings": self.warnings,
        }


class OrderValidationService:
    """
    Centralized validation service for order operations.

    Usage:
        validator = OrderValidationService(db, client_id)
        result = validator.validate_order_data(order_data)
        if not result.is_valid:
            return error_response(result.errors)
    """

    # Validation constants
    PHONE_REGEX = re.compile(r"^[6-9]\d{9}$")
    PINCODE_REGEX = re.compile(r"^\d{6}$")
    EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    GSTIN_REGEX = re.compile(
        r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$"
    )

    # Weight/dimension limits
    MIN_WEIGHT = 0.001  # 1 gram
    MAX_WEIGHT = 100.0  # 100 kg
    MIN_DIMENSION = 0.1  # 0.1 cm
    MAX_DIMENSION = 300.0  # 300 cm (3 meters)

    # Price limits
    MIN_PRICE = 0.0
    MAX_ORDER_VALUE = 10000000.0  # 1 crore

    def __init__(self, db: Session, client_id: int):
        """
        Initialize validation service.

        Args:
            db: Database session
            client_id: Client ID for context-specific validation
        """
        self.db = db
        self.client_id = client_id

    # ============================================
    # MAIN VALIDATION METHODS
    # ============================================

    def validate_order_data(self, order_data: Dict) -> ValidationResult:
        """
        Validate complete order data for creation.

        Args:
            order_data: Dictionary containing order fields

        Returns:
            ValidationResult with all errors/warnings
        """
        result = ValidationResult(is_valid=True)

        # Required field checks
        result.merge(self._validate_required_fields(order_data))

        # Consignee validation
        result.merge(self._validate_consignee(order_data))

        # Billing validation (if not same as consignee)
        if not order_data.get("is_billing_same_as_consignee", True):
            result.merge(self._validate_billing(order_data))

        # Pickup location validation
        result.merge(
            self._validate_pickup_location(order_data.get("pickup_location_code"))
        )

        # Product validation
        result.merge(self._validate_products(order_data.get("products", [])))

        # Package validation
        result.merge(self._validate_package(order_data))

        # Payment validation
        result.merge(self._validate_payment(order_data))

        # Order ID validation
        result.merge(self._validate_order_id(order_data.get("order_id")))

        return result

    def validate_order_id_unique(self, order_id: str) -> ValidationResult:
        """
        Check if order ID is unique for this client.

        Args:
            order_id: Order ID to check

        Returns:
            ValidationResult
        """
        result = ValidationResult(is_valid=True)

        if not order_id:
            result.add_error("order_id", "Order ID is required", "REQUIRED")
            return result

        existing = (
            self.db.query(Order.id, Order.status)
            .filter(
                Order.order_id == order_id,
                Order.client_id == self.client_id,
            )
            .first()
        )

        if existing:
            if existing.status not in ("new", "cancelled"):
                result.add_error(
                    "order_id",
                    f"Order ID '{order_id}' already exists and is processed",
                    "DUPLICATE_PROCESSED",
                    order_id,
                )
            else:
                result.add_warning(
                    f"Order ID '{order_id}' exists with status '{existing.status}'"
                )

        return result

    # ============================================
    # FIELD-LEVEL VALIDATION
    # ============================================

    def _validate_required_fields(self, order_data: Dict) -> ValidationResult:
        """Validate all required fields are present"""
        result = ValidationResult(is_valid=True)

        required_fields = [
            ("order_id", "Order ID"),
            ("order_date", "Order Date"),
            ("consignee_full_name", "Consignee Name"),
            ("consignee_phone", "Consignee Phone"),
            ("consignee_address", "Consignee Address"),
            ("consignee_pincode", "Consignee Pincode"),
            ("consignee_city", "Consignee City"),
            ("consignee_state", "Consignee State"),
            ("pickup_location_code", "Pickup Location"),
            ("payment_mode", "Payment Mode"),
            ("products", "Products"),
            ("weight", "Weight"),
            ("length", "Length"),
            ("breadth", "Breadth"),
            ("height", "Height"),
        ]

        for field_key, field_name in required_fields:
            value = order_data.get(field_key)
            if value is None or (isinstance(value, str) and not value.strip()):
                result.add_error(field_key, f"{field_name} is required", "REQUIRED")
            elif field_key == "products" and (
                not isinstance(value, list) or len(value) == 0
            ):
                result.add_error(
                    field_key, "At least one product is required", "REQUIRED"
                )

        return result

    def _validate_consignee(self, order_data: Dict) -> ValidationResult:
        """Validate consignee details"""
        result = ValidationResult(is_valid=True)

        # Phone validation with centralized normalization
        phone = self.normalize_phone(order_data.get("consignee_phone", ""))

        if not phone:
            result.add_error(
                "consignee_phone",
                "Phone number is required",
                "REQUIRED",
            )
        elif not self.PHONE_REGEX.match(phone):
            result.add_error(
                "consignee_phone",
                "Invalid phone number. Must be 10 digits starting with 6-9",
                "INVALID_PHONE",
                phone,
            )

        # Alternate phone validation (optional) with centralized normalization
        alt_phone_raw = order_data.get("consignee_alternate_phone")
        if alt_phone_raw:
            alt_phone = self.normalize_phone(alt_phone_raw)

            if alt_phone and not self.PHONE_REGEX.match(alt_phone):
                result.add_error(
                    "consignee_alternate_phone",
                    "Invalid alternate phone number. Must be 10 digits starting with 6-9",
                    "INVALID_PHONE",
                    alt_phone,
                )

        # Pincode validation
        pincode = str(order_data.get("consignee_pincode", "")).strip()
        pincode_result = self.validate_pincode(pincode, "consignee_pincode")
        result.merge(pincode_result)

        # Email validation (optional)
        email = order_data.get("consignee_email")
        if email and email.strip():
            if not self.EMAIL_REGEX.match(email.strip()):
                result.add_error(
                    "consignee_email",
                    "Invalid email format",
                    "INVALID_EMAIL",
                    email,
                )

        # GSTIN validation (optional)
        gstin = order_data.get("consignee_gstin")
        if gstin and gstin.strip():
            if not self.GSTIN_REGEX.match(gstin.strip().upper()):
                result.add_error(
                    "consignee_gstin",
                    "Invalid GSTIN format",
                    "INVALID_GSTIN",
                    gstin,
                )

        # Name validation
        name = order_data.get("consignee_full_name", "")
        if name and len(name.strip()) < 2:
            result.add_error(
                "consignee_full_name",
                "Name must be at least 2 characters",
                "INVALID_NAME",
                name,
            )

        # Address validation
        address = order_data.get("consignee_address", "")
        if address and len(address.strip()) < 10:
            result.add_error(
                "consignee_address",
                "Address must be at least 10 characters",
                "INVALID_ADDRESS",
                address,
            )

        return result

    def _validate_billing(self, order_data: Dict) -> ValidationResult:
        """
        Validate billing details when different from consignee.

        FIX: Now includes email format and GSTIN validation for billing address.
        """
        result = ValidationResult(is_valid=True)

        # Required billing fields
        billing_fields = [
            ("billing_full_name", "Billing Name"),
            ("billing_phone", "Billing Phone"),
            ("billing_address", "Billing Address"),
            ("billing_pincode", "Billing Pincode"),
            ("billing_city", "Billing City"),
            ("billing_state", "Billing State"),
        ]

        for field_key, field_name in billing_fields:
            value = order_data.get(field_key)
            if not value or (isinstance(value, str) and not value.strip()):
                result.add_error(field_key, f"{field_name} is required", "REQUIRED")

        # Phone validation with normalization
        phone = order_data.get("billing_phone", "")
        if phone:
            phone = self.normalize_phone(phone)
            if phone and not self.PHONE_REGEX.match(phone):
                result.add_error(
                    "billing_phone",
                    "Invalid billing phone number. Must be 10 digits starting with 6-9",
                    "INVALID_PHONE",
                    phone,
                )

        # Pincode validation
        pincode = order_data.get("billing_pincode", "")
        if pincode:
            pincode_result = self.validate_pincode(
                str(pincode).strip(), "billing_pincode"
            )
            result.merge(pincode_result)

        # FIX: Email validation (if provided)
        billing_email = order_data.get("billing_email")
        if billing_email and billing_email.strip():
            if not self.EMAIL_REGEX.match(billing_email.strip()):
                result.add_error(
                    "billing_email",
                    "Invalid billing email format",
                    "INVALID_EMAIL",
                    billing_email,
                )

        # FIX: GSTIN validation (if provided) - same rules as consignee
        billing_gstin = order_data.get("billing_gstin")
        if billing_gstin and billing_gstin.strip():
            if not self.GSTIN_REGEX.match(billing_gstin.strip().upper()):
                result.add_error(
                    "billing_gstin",
                    "Invalid billing GSTIN format. Must be 15 characters in format: 22AAAAA0000A1Z5",
                    "INVALID_GSTIN",
                    billing_gstin,
                )

        # Name validation
        billing_name = order_data.get("billing_full_name", "")
        if billing_name and len(billing_name.strip()) < 2:
            result.add_error(
                "billing_full_name",
                "Billing name must be at least 2 characters",
                "INVALID_NAME",
                billing_name,
            )

        # Address validation
        billing_address = order_data.get("billing_address", "")
        if billing_address and len(billing_address.strip()) < 10:
            result.add_error(
                "billing_address",
                "Billing address must be at least 10 characters",
                "INVALID_ADDRESS",
                billing_address,
            )

        return result

    def _validate_pickup_location(self, location_code: str) -> ValidationResult:
        """Validate pickup location exists and is active"""
        result = ValidationResult(is_valid=True)

        if not location_code:
            result.add_error(
                "pickup_location_code",
                "Pickup location is required",
                "REQUIRED",
            )
            return result

        # Query database (no caching for pickup locations as they can change)
        pickup = (
            self.db.query(Pickup_Location)
            .filter(
                Pickup_Location.location_code == location_code,
                Pickup_Location.client_id == self.client_id,
                Pickup_Location.is_deleted == False,
            )
            .first()
        )

        if not pickup:
            result.add_error(
                "pickup_location_code",
                f"Pickup location '{location_code}' not found",
                "NOT_FOUND",
                location_code,
            )
            return result

        if not pickup.active:
            result.add_error(
                "pickup_location_code",
                f"Pickup location '{location_code}' is inactive",
                "INACTIVE",
                location_code,
            )

        return result

    def _validate_products(self, products: List[Dict]) -> ValidationResult:
        """Validate product list"""
        result = ValidationResult(is_valid=True)

        if not products or len(products) == 0:
            result.add_error("products", "At least one product is required", "REQUIRED")
            return result

        for i, product in enumerate(products):
            prefix = f"products[{i}]"

            # Name validation
            name = product.get("name", "")
            if not name or not str(name).strip():
                result.add_error(
                    f"{prefix}.name", "Product name is required", "REQUIRED"
                )

            # Quantity validation
            quantity = product.get("quantity", 0)
            try:
                quantity = int(quantity)
                if quantity < 1:
                    result.add_error(
                        f"{prefix}.quantity",
                        "Quantity must be at least 1",
                        "INVALID_QUANTITY",
                        quantity,
                    )
            except (ValueError, TypeError):
                result.add_error(
                    f"{prefix}.quantity",
                    "Invalid quantity value",
                    "INVALID_QUANTITY",
                    quantity,
                )

            # Unit price validation
            unit_price = product.get("unit_price", 0)
            try:
                unit_price = float(unit_price)
                if unit_price < 0:
                    result.add_error(
                        f"{prefix}.unit_price",
                        "Unit price cannot be negative",
                        "INVALID_PRICE",
                        unit_price,
                    )
            except (ValueError, TypeError):
                result.add_error(
                    f"{prefix}.unit_price",
                    "Invalid unit price value",
                    "INVALID_PRICE",
                    unit_price,
                )

        return result

    def _validate_package(self, order_data: Dict) -> ValidationResult:
        """Validate package dimensions and weight"""
        result = ValidationResult(is_valid=True)

        # Weight validation
        weight = order_data.get("weight", 0)
        try:
            weight = float(weight)
            if weight < self.MIN_WEIGHT:
                result.add_error(
                    "weight",
                    f"Weight must be at least {self.MIN_WEIGHT} kg",
                    "WEIGHT_TOO_LOW",
                    weight,
                )
            elif weight > self.MAX_WEIGHT:
                result.add_error(
                    "weight",
                    f"Weight cannot exceed {self.MAX_WEIGHT} kg",
                    "WEIGHT_TOO_HIGH",
                    weight,
                )
        except (ValueError, TypeError):
            result.add_error("weight", "Invalid weight value", "INVALID_WEIGHT", weight)

        # Dimension validation
        dimensions = [
            ("length", order_data.get("length", 0)),
            ("breadth", order_data.get("breadth", 0)),
            ("height", order_data.get("height", 0)),
        ]

        for dim_name, dim_value in dimensions:
            try:
                dim_value = float(dim_value)
                if dim_value < self.MIN_DIMENSION:
                    result.add_error(
                        dim_name,
                        f"{dim_name.capitalize()} must be at least {self.MIN_DIMENSION} cm",
                        "DIMENSION_TOO_LOW",
                        dim_value,
                    )
                elif dim_value > self.MAX_DIMENSION:
                    result.add_error(
                        dim_name,
                        f"{dim_name.capitalize()} cannot exceed {self.MAX_DIMENSION} cm",
                        "DIMENSION_TOO_HIGH",
                        dim_value,
                    )
            except (ValueError, TypeError):
                result.add_error(
                    dim_name,
                    f"Invalid {dim_name} value",
                    "INVALID_DIMENSION",
                    dim_value,
                )

        return result

    def _validate_payment(self, order_data: Dict) -> ValidationResult:
        """Validate payment details"""
        result = ValidationResult(is_valid=True)

        # Payment mode validation
        payment_mode = order_data.get("payment_mode", "").strip().lower()
        valid_modes = ["cod", "prepaid"]

        if payment_mode not in valid_modes:
            result.add_error(
                "payment_mode",
                f"Invalid payment mode. Must be one of: {', '.join(valid_modes)}",
                "INVALID_PAYMENT_MODE",
                payment_mode,
            )

        # Validate charge fields are non-negative
        charge_fields = [
            "shipping_charges",
            "cod_charges",
            "discount",
            "gift_wrap_charges",
            "other_charges",
            "tax_amount",
        ]

        for field in charge_fields:
            value = order_data.get(field, 0)
            try:
                value = float(value)
                if value < 0:
                    result.add_error(
                        field,
                        f"{field.replace('_', ' ').title()} cannot be negative",
                        "NEGATIVE_VALUE",
                        value,
                    )
            except (ValueError, TypeError):
                if value is not None and value != "":
                    result.add_error(
                        field, f"Invalid {field} value", "INVALID_VALUE", value
                    )

        return result

    def _validate_order_id(self, order_id: str) -> ValidationResult:
        """Validate order ID format"""
        result = ValidationResult(is_valid=True)

        if not order_id:
            return result  # Already handled by required fields

        order_id = str(order_id).strip()

        # Length check
        if len(order_id) < 1:
            result.add_error("order_id", "Order ID cannot be empty", "REQUIRED")
        elif len(order_id) > 100:
            result.add_error(
                "order_id",
                "Order ID cannot exceed 100 characters",
                "TOO_LONG",
                order_id,
            )

        # Character check (alphanumeric, dash, underscore)
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", order_id):
            result.add_error(
                "order_id",
                "Order ID can only contain letters, numbers, dash, underscore, and dot",
                "INVALID_CHARACTERS",
                order_id,
            )

        return result

    # ============================================
    # UTILITY METHODS
    # ============================================

    @staticmethod
    def normalize_phone(phone: str) -> str:
        """
        Normalize phone number by removing country code prefix and non-digit characters.

        FIX: Improved phone normalization to handle:
        - Country code prefixes (+91, 91, 0)
        - Spaces and dashes in phone numbers
        - Other non-digit characters

        Args:
            phone: Raw phone number

        Returns:
            Normalized 10-digit phone number or empty string
        """
        if not phone:
            return ""

        phone = str(phone).strip()

        # First, remove all non-digit characters (handles spaces, dashes, etc.)
        phone = re.sub(r'\D', '', phone)

        # Remove country code prefixes
        if phone.startswith("91") and len(phone) > 10:
            phone = phone[2:]
        elif phone.startswith("0") and len(phone) == 11:
            phone = phone[1:]

        # Take last 10 digits if still too long
        if len(phone) > 10:
            phone = phone[-10:]

        return phone
    
    @staticmethod
    def sanitize_string(value: str, max_length: int = None) -> str:
        """
        Sanitize string input by normalizing unicode and trimming.

        FIX: Ensures consistent unicode representation and prevents
        display issues with special characters.

        Args:
            value: Raw string value
            max_length: Optional maximum length to truncate to

        Returns:
            Sanitized string
        """
        if not value:
            return ""
        
        value = str(value).strip()
        
        # Normalize unicode to NFKC form (compatibility composition)
        # This converts special unicode characters to their standard equivalents
        value = unicodedata.normalize('NFKC', value)
        
        # Remove control characters (except newlines and tabs)
        value = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', value)
        
        # Truncate if max_length specified
        if max_length and len(value) > max_length:
            value = value[:max_length]
        
        return value

    def validate_pincode(
        self, pincode: str, field_name: str = "pincode"
    ) -> ValidationResult:
        """
        Validate pincode format and check serviceability.

        FIX: Better error messages distinguishing between format and serviceability issues.

        Args:
            pincode: Pincode to validate
            field_name: Field name for error reporting

        Returns:
            ValidationResult
        """
        result = ValidationResult(is_valid=True)

        if not pincode:
            result.add_error(field_name, "Pincode is required", "REQUIRED")
            return result

        pincode = str(pincode).strip()

        # Format check - must be exactly 6 digits
        if not self.PINCODE_REGEX.match(pincode):
            # Provide more specific error message
            if len(pincode) != 6:
                result.add_error(
                    field_name,
                    f"Pincode must be exactly 6 digits (got {len(pincode)} digits)",
                    "INVALID_FORMAT",
                    pincode,
                )
            elif not pincode.isdigit():
                result.add_error(
                    field_name,
                    "Pincode must contain only digits",
                    "INVALID_FORMAT",
                    pincode,
                )
            else:
                result.add_error(
                    field_name,
                    "Invalid pincode format",
                    "INVALID_FORMAT",
                    pincode,
                )
            return result

        # Use module-level cached function for pincode lookup
        pincode_data = _get_pincode_data(self.db, pincode)

        if not pincode_data:
            result.add_error(
                field_name,
                f"Pincode '{pincode}' is not serviceable. Please check if this is a valid delivery location.",
                "NOT_SERVICEABLE",
                pincode,
            )

        return result

    def get_pincode_details(self, pincode: str) -> Optional[Tuple[str, str]]:
        """
        Get city and state for a pincode.

        Args:
            pincode: Pincode to lookup

        Returns:
            Tuple of (city, state) or None if not found
        """
        pincode = str(pincode).strip()
        data = _get_pincode_data(self.db, pincode)
        return (data.city, data.state) if data else None

    def get_pickup_location(self, location_code: str) -> Optional[Pickup_Location]:
        """
        Get pickup location by code.

        Args:
            location_code: Location code to lookup

        Returns:
            Pickup_Location or None
        """
        if not location_code:
            return None

        # Query database directly (pickup locations can change, avoid stale cache)
        pickup = (
            self.db.query(Pickup_Location)
            .filter(
                Pickup_Location.location_code == location_code,
                Pickup_Location.client_id == self.client_id,
                Pickup_Location.is_deleted == False,
            )
            .first()
        )

        return pickup


# ============================================
# MODULE-LEVEL CACHED FUNCTIONS
# ============================================
# Using TTLCache for thread-safe pincode lookups with automatic expiration.
# Note: Pickup locations are NOT cached as they can change (active/inactive status).


def _get_pincode_data(db: Session, pincode: str):
    """
    Get pincode data from database with thread-safe caching.

    FIX: Uses TTLCache with Lock for thread-safety.
    Cache entries expire after 1 hour automatically.

    Args:
        db: Database session
        pincode: Pincode to lookup

    Returns:
        Tuple of (city, state) or None if not found
    """
    # Thread-safe cache check
    with _cache_lock:
        if pincode in _pincode_cache:
            return _pincode_cache[pincode]

    # Query database (outside lock to avoid blocking)
    data = (
        db.query(Pincode_Mapping.city, Pincode_Mapping.state)
        .filter(Pincode_Mapping.pincode == pincode)
        .first()
    )

    # Thread-safe cache update
    with _cache_lock:
        _pincode_cache[pincode] = data

    return data


def clear_pincode_cache():
    """Clear the pincode cache. Useful for testing or after data updates."""
    with _cache_lock:
        _pincode_cache.clear()
