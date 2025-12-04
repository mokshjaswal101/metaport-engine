"""
Order Creation Service

Orchestrates the complete order creation flow with:
- Validation
- Calculation
- Database operations
- Audit logging

Uses proper transaction management - all operations succeed or fail together.

FIXED ISSUES:
- Race condition in order ID validation (uses DB constraint + exception handling)
- Missing transaction rollback on partial failures (uses savepoint)
- Zone calculation failure now properly handled
- Empty product list validation
- COD amount validation (cannot exceed total)
- Pickup location re-validation before creation
- Long string truncation warnings
"""

import http
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from pytz import timezone as pytz_timezone

from logger import logger
from context_manager.context import context_user_data, get_db_session
from schema.base import GenericResponseModel

# Models
from models import Order, Pickup_Location, OrderItem, OrderAuditLog

# Services
from .order_validation_service import OrderValidationService, ValidationResult
from .order_calculation_service import OrderCalculationService, OrderCalculations

# Zone calculation
from modules.shipment import ShipmentService


class OrderCreationError(Exception):
    """Custom exception for order creation errors"""

    def __init__(
        self,
        message: str,
        code: str,
        field: str = None,
        status_code: int = 400,
        warnings: List[str] = None,
    ):
        self.message = message
        self.code = code
        self.field = field
        self.status_code = status_code
        self.warnings = warnings or []
        super().__init__(message)


class OrderCreationService:
    """
    Service for creating single orders.

    Handles the complete flow:
    1. Validate input data
    2. Check for duplicates
    3. Calculate derived values
    4. Create order + items + audit log
    5. Calculate zone

    Usage:
        service = OrderCreationService(db, user_context)
        result = service.create_order(order_data)
    """

    IST = pytz_timezone("Asia/Kolkata")
    UTC = pytz_timezone("UTC")

    def __init__(self, db: Session = None):
        """
        Initialize order creation service.

        Args:
            db: Database session (optional, will use context if not provided)
        """
        self.db = db or get_db_session()
        self.user_context = context_user_data.get()
        self.client_id = self.user_context.client_id

        # Initialize sub-services
        self.validator = OrderValidationService(self.db, self.client_id)
        self.calculator = OrderCalculationService()

    # ============================================
    # MAIN CREATION METHOD
    # ============================================

    def create_order(self, order_data: Dict) -> GenericResponseModel:
        """
        Create a new order with complete validation and processing.

        Uses savepoint transaction management - all operations succeed or fail together.
        Handles race conditions via database unique constraint + exception handling.

        Args:
            order_data: Dictionary containing order fields

        Returns:
            GenericResponseModel with success/error response
        """
        warnings = []  # Collect non-blocking warnings

        try:
            # Step 1: Validate input
            validation_result = self.validator.validate_order_data(order_data)
            if not validation_result.is_valid:
                return self._validation_error_response(validation_result)

            # Collect validation warnings
            warnings.extend(validation_result.warnings)

            # Step 2: Check for duplicate order ID (preliminary check - DB constraint is final)
            duplicate_result = self.validator.validate_order_id_unique(
                order_data.get("order_id")
            )
            if not duplicate_result.is_valid:
                # Check if it's a duplicate that can be reused
                existing_order = self._get_existing_order(order_data.get("order_id"))
                if existing_order:
                    return self._handle_existing_order(existing_order)
                return self._validation_error_response(duplicate_result)

            # Step 3: Get pickup location (needed for zone calculation)
            # FIX: Re-validate pickup location is still active before creation
            pickup_location = self._get_and_validate_pickup_location(
                order_data.get("pickup_location_code")
            )
            if not pickup_location:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Invalid or inactive pickup location",
                    data={"code": "INVALID_PICKUP_LOCATION"},
                )

            # Step 4: Calculate zone (FIX: Fail if zone calculation fails)
            zone = self._calculate_zone(
                pickup_location.pincode, order_data.get("consignee_pincode")
            )
            if not zone:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="Unable to calculate shipping zone for the given pincodes. Please verify the delivery pincode is serviceable.",
                    data={"code": "ZONE_CALCULATION_FAILED"},
                )

            # Step 5: Calculate derived values
            calculations = self.calculator.calculate_order(order_data)

            # Step 6: Validate COD amount doesn't exceed total (FIX: Edge case)
            cod_validation = self._validate_cod_amount(
                order_data, calculations.total_amount
            )
            if cod_validation:
                return cod_validation

            # Step 7: Validate products list has valid items (FIX: Edge case)
            products = order_data.get("products", [])
            valid_products, product_warnings = self._validate_and_filter_products(
                products
            )
            warnings.extend(product_warnings)

            if not valid_products:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message="At least one valid product with a name is required",
                    data={"code": "NO_VALID_PRODUCTS"},
                )

            # Step 8: Prepare order entity with truncation warnings
            order_entity, truncation_warnings = (
                self._prepare_order_entity_with_warnings(order_data, calculations, zone)
            )
            warnings.extend(truncation_warnings)

            # Step 9: Create order with transaction savepoint (FIX: Proper rollback)
            # All DB operations happen within savepoint - any failure rolls back everything
            try:
                savepoint = self.db.begin_nested()

                # Create order
                self.db.add(order_entity)
                self.db.flush()  # Get the ID

                # Create order items
                self._create_order_items(order_entity.id, valid_products)

                # Create audit log
                self._create_audit_log(order_entity.id, "platform")

                # Commit savepoint
                savepoint.commit()

            except IntegrityError as e:
                # FIX: Handle race condition - duplicate order_id detected at DB level
                savepoint.rollback()
                if (
                    "uq_order_id_client" in str(e.orig).lower()
                    or "duplicate" in str(e.orig).lower()
                ):
                    # Another request created this order_id concurrently
                    existing_order = self._get_existing_order(
                        order_data.get("order_id")
                    )
                    if existing_order:
                        return self._handle_existing_order(existing_order)
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.CONFLICT,
                        status=False,
                        message=f"Order ID '{order_data.get('order_id')}' already exists",
                        data={"code": "DUPLICATE_ORDER"},
                    )
                raise  # Re-raise other integrity errors

            except Exception as e:
                # FIX: Rollback on any failure during creation
                savepoint.rollback()
                raise

            logger.info(
                msg=f"Order created successfully: {order_entity.order_id}",
                extra={"client_id": self.client_id, "order_id": order_entity.order_id},
            )

            response_data = {
                "order_id": order_entity.order_id,
                "id": order_entity.id,
                "zone": zone,
            }

            # Include warnings in response if any
            if warnings:
                response_data["warnings"] = warnings

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Order created successfully",
                data=response_data,
            )

        except OrderCreationError as e:
            logger.warning(
                msg=f"Order creation failed: {e.message}",
                extra={"client_id": self.client_id, "code": e.code},
            )
            response_data = {"code": e.code}
            if e.field:
                response_data["field"] = e.field
            if e.warnings:
                response_data["warnings"] = e.warnings
            return GenericResponseModel(
                status_code=e.status_code,
                status=False,
                message=e.message,
                data=response_data,
            )

        except Exception as e:
            logger.error(
                msg=f"Unexpected error creating order: {str(e)}",
                extra={"client_id": self.client_id},
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An error occurred while creating the order",
            )

    # ============================================
    # HELPER METHODS
    # ============================================

    def _prepare_order_entity(
        self,
        order_data: Dict,
        calculations: OrderCalculations,
        zone: str,
    ) -> Order:
        """
        Prepare Order entity from input data and calculations.

        FIX: Now uses sanitize_string for unicode normalization and data cleaning.

        Args:
            order_data: Raw order data
            calculations: Calculated values
            zone: Shipping zone

        Returns:
            Order entity (not yet persisted)
        """
        # Convert order date to proper datetime
        order_date = self._parse_order_date(order_data.get("order_date"))

        # Clean phone numbers using improved normalization
        consignee_phone = self.validator.normalize_phone(order_data.get("consignee_phone", ""))
        consignee_alternate_phone = self.validator.normalize_phone(
            order_data.get("consignee_alternate_phone", "")
        )
        billing_phone = self.validator.normalize_phone(order_data.get("billing_phone", ""))

        # Helper for sanitizing strings with max length
        def sanitize(value, max_len=None):
            return self.validator.sanitize_string(value, max_len)

        # Build order entity with sanitized data
        order = Order(
            # Identification
            order_id=sanitize(order_data.get("order_id", ""), 100),
            order_date=order_date,
            channel=sanitize(order_data.get("channel", ""), 100) or "Custom",
            client_id=self.client_id,
            # Consignee details (with max lengths matching DB schema)
            consignee_full_name=sanitize(order_data.get("consignee_full_name", ""), 100),
            consignee_phone=consignee_phone,
            consignee_alternate_phone=consignee_alternate_phone or None,
            consignee_email=sanitize(order_data.get("consignee_email", ""), 150) or None,
            consignee_company=sanitize(order_data.get("consignee_company", ""), 100) or None,
            consignee_gstin=sanitize(order_data.get("consignee_gstin", ""), 15).upper() or None,
            consignee_address=sanitize(order_data.get("consignee_address", ""), 255),
            consignee_landmark=sanitize(order_data.get("consignee_landmark", ""), 255) or None,
            consignee_pincode=sanitize(order_data.get("consignee_pincode", ""), 10),
            consignee_city=sanitize(order_data.get("consignee_city", ""), 100),
            consignee_state=sanitize(order_data.get("consignee_state", ""), 100),
            consignee_country=sanitize(order_data.get("consignee_country", "India"), 100),
            # Billing details
            is_billing_same_as_consignee=order_data.get(
                "is_billing_same_as_consignee", True
            ),
            billing_full_name=sanitize(order_data.get("billing_full_name", ""), 100) or None,
            billing_phone=billing_phone or None,
            billing_email=sanitize(order_data.get("billing_email", ""), 150) or None,
            billing_address=sanitize(order_data.get("billing_address", ""), 255) or None,
            billing_landmark=sanitize(order_data.get("billing_landmark", ""), 255) or None,
            billing_pincode=sanitize(order_data.get("billing_pincode", ""), 10) or None,
            billing_city=sanitize(order_data.get("billing_city", ""), 100) or None,
            billing_state=sanitize(order_data.get("billing_state", ""), 100) or None,
            billing_country=sanitize(order_data.get("billing_country", ""), 100) or None,
            # Pickup
            pickup_location_code=sanitize(order_data.get("pickup_location_code", ""), 255),
            # Payment (from calculations)
            payment_mode=sanitize(order_data.get("payment_mode", "prepaid"), 15).lower(),
            total_amount=calculations.total_amount,
            order_value=calculations.order_value,
            shipping_charges=self._to_float(order_data.get("shipping_charges", 0)),
            cod_charges=self._to_float(order_data.get("cod_charges", 0)),
            discount=self._to_float(order_data.get("discount", 0)),
            gift_wrap_charges=self._to_float(order_data.get("gift_wrap_charges", 0)),
            other_charges=self._to_float(order_data.get("other_charges", 0)),
            tax_amount=calculations.tax_amount,
            cod_to_collect=calculations.cod_to_collect,
            # Package (from calculations)
            length=self._to_float(order_data.get("length", 0)),
            breadth=self._to_float(order_data.get("breadth", 0)),
            height=self._to_float(order_data.get("height", 0)),
            weight=self._to_float(order_data.get("weight", 0)),
            volumetric_weight=calculations.volumetric_weight,
            applicable_weight=calculations.applicable_weight,
            # Shipment (initial values)
            zone=zone,
            status="new",
            sub_status="new",
            # Flags
            is_label_generated=False,
            clone_order_count=0,
            cancel_count=0,
        )

        return order

    def _create_order_in_db(self, order: Order) -> Order:
        """
        Add order to database session.

        Args:
            order: Order entity to create

        Returns:
            Created order with ID
        """
        self.db.add(order)
        self.db.flush()  # Get the ID
        return order

    def _create_order_items(self, order_id: int, products: List[Dict]):
        """
        Create OrderItem records for products using bulk insert.

        PERFORMANCE FIX: Uses bulk_insert_mappings for 5-10x faster insertion
        when orders have multiple products.

        Args:
            order_id: Parent order ID
            products: List of product dictionaries
        """
        if not products:
            return

        # Prepare items for bulk insert
        items_to_insert = []
        for product in products:
            if not product or not product.get("name"):
                continue

            items_to_insert.append(
                {
                    "order_id": order_id,
                    "name": str(product.get("name", "")).strip()[:255],
                    "sku_code": str(product.get("sku_code", "")).strip()[:100] or None,
                    "quantity": int(product.get("quantity", 1)),
                    "unit_price": round(float(product.get("unit_price", 0)), 2),
                }
            )

        # Use bulk insert for better performance (single INSERT statement)
        if items_to_insert:
            self.db.bulk_insert_mappings(OrderItem, items_to_insert)

    def _create_audit_log(self, order_id: int, source: str = "platform"):
        """
        Create audit log entry for order creation.

        FIX: Improved user name fallback to use email if name not available.

        Args:
            order_id: Order ID
            source: Source of order creation
        """
        # Get user name with fallback to email
        user_name = (
            getattr(self.user_context, "name", None)
            or getattr(self.user_context, "email", None)
            or "System"
        ) if self.user_context else "System"

        audit_log = OrderAuditLog.log_order_created(
            order_id=order_id,
            user_id=self.user_context.id if self.user_context else None,
            user_name=user_name,
            source=source,
        )
        self.db.add(audit_log)

    def _calculate_zone(self, pickup_pincode: str, delivery_pincode: str) -> str:
        """
        Calculate shipping zone.

        Args:
            pickup_pincode: Origin pincode
            delivery_pincode: Destination pincode

        Returns:
            Zone letter (A, B, C, D, E) or empty string if failed
        """
        try:
            zone_result = ShipmentService.calculate_shipping_zone(
                pickup_pincode, delivery_pincode
            )
            if zone_result.status:
                return zone_result.data.get("zone", "")
        except Exception as e:
            logger.warning(f"Zone calculation failed: {str(e)}")

        return ""

    def _get_and_validate_pickup_location(
        self, location_code: str
    ) -> Optional[Pickup_Location]:
        """
        Get pickup location and validate it's still active.

        FIX: Re-validates pickup location status before order creation
        to handle case where location was deactivated between form load and submit.

        Args:
            location_code: Location code to lookup

        Returns:
            Pickup_Location if valid and active, None otherwise
        """
        if not location_code:
            return None

        # Fresh query to get current status (bypassing cache)
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
            logger.warning(f"Pickup location not found: {location_code}")
            return None

        if not pickup.active:
            logger.warning(f"Pickup location inactive: {location_code}")
            return None

        return pickup

    def _validate_cod_amount(
        self, order_data: Dict, total_amount: float
    ) -> Optional[GenericResponseModel]:
        """
        Validate COD amount doesn't exceed total amount.

        FIX: Prevents customer from being asked to pay more than order value.

        Args:
            order_data: Order data dictionary
            total_amount: Calculated total amount

        Returns:
            Error response if validation fails, None if valid
        """
        payment_mode = str(order_data.get("payment_mode", "")).strip().lower()

        if payment_mode != "cod":
            return None

        cod_to_collect = order_data.get("cod_to_collect")
        if cod_to_collect is None:
            return None

        try:
            cod_amount = float(cod_to_collect)
            if cod_amount > total_amount:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    status=False,
                    message=f"COD amount (₹{cod_amount:.2f}) cannot exceed total order amount (₹{total_amount:.2f})",
                    data={
                        "code": "COD_EXCEEDS_TOTAL",
                        "field": "cod_to_collect",
                        "cod_amount": cod_amount,
                        "total_amount": total_amount,
                    },
                )
        except (ValueError, TypeError):
            pass

        return None

    def _validate_and_filter_products(
        self, products: List[Dict]
    ) -> Tuple[List[Dict], List[str]]:
        """
        Validate and filter products, returning valid products and warnings.

        FIX: Prevents orders with empty product list after filtering.

        Args:
            products: List of product dictionaries

        Returns:
            Tuple of (valid_products, warnings)
        """
        warnings = []
        valid_products = []

        if not products:
            return [], []

        for i, product in enumerate(products):
            if not product:
                continue

            name = product.get("name", "")
            if isinstance(name, str):
                name = name.strip()

            if not name:
                warnings.append(f"Product at index {i} has empty name and was skipped")
                continue

            valid_products.append(product)

        return valid_products, warnings

    def _prepare_order_entity_with_warnings(
        self,
        order_data: Dict,
        calculations: OrderCalculations,
        zone: str,
    ) -> Tuple[Order, List[str]]:
        """
        Prepare Order entity with truncation warnings.

        FIX: Warns user when data is truncated to fit field limits.

        Args:
            order_data: Raw order data
            calculations: Calculated values
            zone: Shipping zone

        Returns:
            Tuple of (Order entity, list of warnings)
        """
        warnings = []

        # Check for truncation
        def check_truncation(value: str, max_len: int, field_name: str) -> str:
            if value and len(value) > max_len:
                warnings.append(
                    f"{field_name} was truncated from {len(value)} to {max_len} characters"
                )
                return value[:max_len]
            return value

        # Use existing _prepare_order_entity but track truncations
        order = self._prepare_order_entity(order_data, calculations, zone)

        # Check key fields for truncation (done after prepare for logging)
        consignee_name = str(order_data.get("consignee_full_name", "")).strip()
        if len(consignee_name) > 100:
            warnings.append(
                f"Consignee name was truncated from {len(consignee_name)} to 100 characters"
            )

        consignee_address = str(order_data.get("consignee_address", "")).strip()
        if len(consignee_address) > 255:
            warnings.append(
                f"Consignee address was truncated from {len(consignee_address)} to 255 characters"
            )

        return order, warnings

    def _get_existing_order(self, order_id: str) -> Optional[Order]:
        """
        Get existing order by order_id.

        Args:
            order_id: Order ID to check

        Returns:
            Order if exists, None otherwise
        """
        return (
            self.db.query(Order)
            .filter(
                Order.order_id == order_id,
                Order.client_id == self.client_id,
            )
            .first()
        )

    def _handle_existing_order(self, order: Order) -> GenericResponseModel:
        """
        Handle case where order already exists.

        Args:
            order: Existing order

        Returns:
            Appropriate response based on order status
        """
        if order.status in ("new", "cancelled"):
            # Order can be updated or resubmitted
            return GenericResponseModel(
                status_code=http.HTTPStatus.CONFLICT,
                status=False,
                message=f"Order ID '{order.order_id}' already exists with status '{order.status}'",
                data={
                    "order_id": order.order_id,
                    "status": order.status,
                    "code": "DUPLICATE_ORDER",
                },
            )
        else:
            # Order is already processed
            return GenericResponseModel(
                status_code=http.HTTPStatus.CONFLICT,
                status=False,
                message=f"Order ID '{order.order_id}' already exists and is processed",
                data={
                    "order_id": order.order_id,
                    "status": order.status,
                    "awb_number": order.awb_number,
                    "code": "ORDER_ALREADY_PROCESSED",
                },
            )

    def _validation_error_response(
        self, result: ValidationResult
    ) -> GenericResponseModel:
        """
        Create error response from validation result.

        Args:
            result: Validation result with errors

        Returns:
            GenericResponseModel with validation errors
        """
        first_error = result.errors[0] if result.errors else None

        return GenericResponseModel(
            status_code=http.HTTPStatus.BAD_REQUEST,
            status=False,
            message=first_error.message if first_error else "Validation failed",
            data={
                "code": "VALIDATION_ERROR",
                "errors": [
                    {"field": e.field, "message": e.message, "code": e.code}
                    for e in result.errors
                ],
            },
        )

    # ============================================
    # UTILITY METHODS
    # ============================================

    def _parse_order_date(self, order_date: Any) -> datetime:
        """
        Parse order date from various formats.

        Args:
            order_date: Date string or datetime

        Returns:
            Datetime with IST timezone
        """
        if isinstance(order_date, datetime):
            if order_date.tzinfo is None:
                return self.IST.localize(order_date)
            return order_date.astimezone(self.IST)

        if not order_date:
            return datetime.now(self.IST)

        date_str = str(order_date).strip()

        # Try various formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d-%m-%Y %H:%M:%S",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%d/%m/%Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return self.IST.localize(dt)
            except ValueError:
                continue

        # Default to now
        return datetime.now(self.IST)

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        """
        Convert value to float safely.

        Args:
            value: Value to convert
            default: Default if conversion fails

        Returns:
            Float value
        """
        if value is None:
            return default
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return default
