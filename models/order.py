"""
Order Model (Optimized)

This is the core order table, optimized for high-volume operations.
Fields have been normalized into separate tables:
- Products → order_item table
- Tracking events → order_tracking table
- Audit history → order_audit_log table
- Billing/freight → order_billing table

Removed fields:
- company_id (using client_id only)
- order_type (B2B has separate table)
- store_id (deferred to channels module)
- request (raw channel data)
- invoice fields (deferred)
- tracking_info, tracking_response (moved to order_tracking)
- products (moved to order_item)
- action_history (moved to order_audit_log)
- All freight fields (moved to order_billing)
- source, marketplace_order_id (removed)
- order_tags (will be separate table)
- tracking_id (removed)

Field naming conventions:
- Price fields: Numeric(10, 2) - 2 decimal places
- Weight/dimension fields: Numeric(10, 3) - 3 decimal places
- All datetime fields: TIMESTAMP with timezone
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    Numeric,
    Boolean,
    TIMESTAMP,
    UniqueConstraint,
    Index,
    func,
    select,
)
from sqlalchemy.orm import relationship, column_property, joinedload, selectinload
from sqlalchemy.ext.hybrid import hybrid_property

from pytz import timezone as pytz_timezone

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class Order(DBBase, DBBaseClass):

    __tablename__ = "order"

    # ============================================
    # ORDER IDENTIFICATION
    # ============================================

    order_id = Column(String(255), nullable=False)
    order_date = Column(TIMESTAMP(timezone=True), nullable=False)
    channel = Column(String(100), nullable=True)

    # Client reference (using client_id only, no company_id)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    # ============================================
    # CONSIGNEE DETAILS
    # ============================================

    consignee_full_name = Column(String(100), nullable=False)
    consignee_phone = Column(String(15), nullable=False)
    consignee_alternate_phone = Column(String(15), nullable=True)
    consignee_email = Column(String(150), nullable=True)
    consignee_company = Column(String(100), nullable=True)
    consignee_gstin = Column(String(15), nullable=True)
    consignee_address = Column(String(255), nullable=False)
    consignee_landmark = Column(String(255), nullable=True)
    consignee_pincode = Column(String(10), nullable=False)
    consignee_city = Column(String(100), nullable=False)
    consignee_state = Column(String(100), nullable=False)
    consignee_country = Column(String(100), nullable=False)

    # ============================================
    # BILLING DETAILS (when different from consignee)
    # ============================================

    is_billing_same_as_consignee = Column(Boolean, nullable=True, default=True)
    billing_full_name = Column(String(100), nullable=True)
    billing_phone = Column(String(15), nullable=True)
    billing_email = Column(String(150), nullable=True)
    billing_address = Column(String(255), nullable=True)
    billing_landmark = Column(String(255), nullable=True)
    billing_pincode = Column(String(10), nullable=True)
    billing_city = Column(String(100), nullable=True)
    billing_state = Column(String(100), nullable=True)
    billing_country = Column(String(100), nullable=True)

    # ============================================
    # PICKUP DETAILS
    # ============================================

    pickup_location_code = Column(
        String(255),
        ForeignKey("pickup_location.location_code"),
        nullable=False,
    )

    # ============================================
    # PAYMENT DETAILS (2 decimal places for prices)
    # ============================================

    payment_mode = Column(String(15), nullable=False)
    total_amount = Column(Numeric(10, 2), nullable=False)
    order_value = Column(Numeric(10, 2), nullable=False)
    shipping_charges = Column(Numeric(10, 2), nullable=True, default=0)
    cod_charges = Column(Numeric(10, 2), nullable=True, default=0)
    discount = Column(Numeric(10, 2), nullable=True, default=0)
    gift_wrap_charges = Column(Numeric(10, 2), nullable=True, default=0)
    other_charges = Column(Numeric(10, 2), nullable=True, default=0)
    tax_amount = Column(Numeric(10, 2), nullable=True, default=0)

    # COD amount to collect (for partial COD scenarios)
    cod_to_collect = Column(Numeric(10, 2), nullable=True, default=0)

    # E-way bill number (mandatory for orders >= ₹50,000)
    eway_bill_number = Column(String(12), nullable=True)

    # ============================================
    # PACKAGE DETAILS (3 decimal places for dimensions/weight)
    # ============================================

    # Dimensions (cm) - 3 decimal places
    length = Column(Numeric(10, 3), nullable=False)
    breadth = Column(Numeric(10, 3), nullable=False)
    height = Column(Numeric(10, 3), nullable=False)

    # Weight (kg) - 3 decimal places
    weight = Column(Numeric(10, 3), nullable=False)
    applicable_weight = Column(Numeric(10, 3), nullable=False)
    volumetric_weight = Column(Numeric(10, 3), nullable=False)

    # ============================================
    # SHIPMENT DETAILS
    # ============================================

    # Courier/Partner info
    aggregator = Column(String(255), nullable=True)
    courier_partner = Column(String(255), nullable=True)
    shipment_mode = Column(String(255), nullable=True)
    awb_number = Column(String(255), nullable=True)
    shipping_partner_order_id = Column(String(255), nullable=True)
    shipping_partner_shipping_id = Column(String(255), nullable=True)

    # Zone
    zone = Column(String(1), nullable=True)

    # Document URLs
    manifest_url = Column(String(500), nullable=True)
    label_url = Column(String(500), nullable=True)
    invoice_url = Column(String(500), nullable=True)

    # ============================================
    # STATUS TRACKING
    # ============================================

    status = Column(String(50), nullable=False, default="new")
    sub_status = Column(String(50), nullable=False, default="new")
    courier_status = Column(String(100), nullable=True)

    # Status error tracking
    shipment_booking_error = Column(String(500), nullable=True)
    pickup_failed_reason = Column(String(255), nullable=True)
    rto_reason = Column(String(255), nullable=True)

    # ============================================
    # STATUS DATES (timezone aware - kept for filtering)
    # ============================================

    booking_date = Column(TIMESTAMP(timezone=True), nullable=True)
    pickup_completion_date = Column(TIMESTAMP(timezone=True), nullable=True)
    first_ofp_date = Column(TIMESTAMP(timezone=True), nullable=True)
    shipped_date = Column(TIMESTAMP(timezone=True), nullable=True)
    edd = Column(TIMESTAMP(timezone=True), nullable=True)
    first_ofd_date = Column(TIMESTAMP(timezone=True), nullable=True)
    delivered_date = Column(TIMESTAMP(timezone=True), nullable=True)
    rto_initiated_date = Column(TIMESTAMP(timezone=True), nullable=True)
    rto_delivered_date = Column(TIMESTAMP(timezone=True), nullable=True)
    last_update_date = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        default=lambda: datetime.now(pytz_timezone("Asia/Kolkata")),
    )

    # ============================================
    # FLAGS & METADATA
    # ============================================

    is_label_generated = Column(Boolean, nullable=False, default=False)

    # Clone/cancel tracking
    clone_order_count = Column(Integer, nullable=False, default=0)
    cancel_count = Column(Integer, nullable=False, default=0)

    # COD remittance linking
    cod_remittance_cycle_id = Column(
        Integer,
        ForeignKey("cod_remittance.id"),
        nullable=True,
    )

    # ============================================
    # RELATIONSHIPS
    # ============================================

    # Core relationships
    client = relationship("Client", back_populates="orders", lazy="noload")
    pickup_location = relationship("Pickup_Location", lazy="noload")

    # Normalized table relationships
    items = relationship(
        "OrderItem",
        back_populates="order",
        lazy="noload",
        cascade="all, delete-orphan",
    )
    tracking_events = relationship(
        "OrderTracking",
        back_populates="order",
        lazy="noload",
        cascade="all, delete-orphan",
        order_by="OrderTracking.event_datetime.desc()",
    )
    audit_logs = relationship(
        "OrderAuditLog",
        back_populates="order",
        lazy="noload",
        cascade="all, delete-orphan",
        order_by="OrderAuditLog.timestamp.desc()",
    )
    billing = relationship(
        "OrderBilling",
        back_populates="order",
        lazy="noload",
        uselist=False,
        cascade="all, delete-orphan",
    )

    # Existing relationships
    ndr = relationship("Ndr", back_populates="order", lazy="noload")
    courier_billing = relationship(
        "CourierBilling", back_populates="order", lazy="noload"
    )
    rate_discrepancies = relationship(
        "Admin_Rate_Discrepancie", back_populates="order", lazy="noload"
    )

    # ============================================
    # INDEXES & CONSTRAINTS
    # ============================================

    __table_args__ = (
        # Unique constraint: order_id must be unique per client
        UniqueConstraint("order_id", "client_id", name="uq_order_id_client_v2"),
        # FIX: AWB number must be unique (prevents duplicate AWB assignment)
        # Note: awb_number can be NULL initially, unique constraint only applies to non-NULL values
        Index(
            "ix_order_v2_awb_unique",
            "awb_number",
            unique=True,
            postgresql_where="awb_number IS NOT NULL",
        ),
        # Primary query indexes
        Index("ix_order_v2_client_status", "client_id", "status", "is_deleted"),
        Index("ix_order_v2_client_date", "client_id", "order_date"),
        Index("ix_order_v2_client_booking", "client_id", "booking_date"),
        # Search indexes
        Index("ix_order_v2_awb", "awb_number"),
        Index("ix_order_v2_phone", "consignee_phone"),
        Index("ix_order_v2_lookup", "order_id", "client_id"),
        # FIX: Composite index for phone search with client_id (performance optimization)
        Index("ix_order_v2_phone_client", "consignee_phone", "client_id"),
        # Filter indexes
        Index("ix_order_v2_pickup", "pickup_location_code", "client_id"),
        Index("ix_order_v2_courier", "courier_partner", "client_id"),
        Index("ix_order_v2_payment", "payment_mode", "client_id"),
        Index("ix_order_v2_pincode", "consignee_pincode", "client_id"),
        # Composite index for common list query
        Index(
            "ix_order_v2_list",
            "client_id",
            "is_deleted",
            "status",
            "order_date",
        ),
        Index(
            "ix_order_v2_client_date_status",
            "client_id",
            "order_date",
            "status",
            "is_deleted",
        ),
        Index(
            "ix_order_v2_new_orders",
            "client_id",
            "created_at",
            postgresql_where="status = 'new' AND is_deleted = false",
        ),
    )

    # ============================================
    # METHODS
    # ============================================

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "uuid": str(self.uuid),
            "order_id": self.order_id,
            "order_date": self.order_date.isoformat() if self.order_date else None,
            "channel": self.channel,
            "client_id": self.client_id,
            "consignee_full_name": self.consignee_full_name,
            "consignee_phone": self.consignee_phone,
            "consignee_email": self.consignee_email,
            "consignee_address": self.consignee_address,
            "consignee_pincode": self.consignee_pincode,
            "consignee_city": self.consignee_city,
            "consignee_state": self.consignee_state,
            "payment_mode": self.payment_mode,
            "total_amount": round(float(self.total_amount or 0), 2),
            "order_value": round(float(self.order_value or 0), 2),
            "cod_to_collect": round(float(self.cod_to_collect or 0), 2),
            "weight": round(float(self.weight or 0), 3),
            "applicable_weight": round(float(self.applicable_weight or 0), 3),
            "volumetric_weight": round(float(self.volumetric_weight or 0), 3),
            "awb_number": self.awb_number,
            "courier_partner": self.courier_partner,
            "status": self.status,
            "sub_status": self.sub_status,
            "zone": self.zone,
            "pickup_location_code": self.pickup_location_code,
            "eway_bill_number": self.eway_bill_number,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def _serialize_pickup_location(self):
        """Convert pickup_location relationship to serializable dict."""
        if self.pickup_location is None:
            return None
        pl = self.pickup_location
        return {
            "location_code": pl.location_code,
            "location_name": pl.location_name,
            "contact_person_name": pl.contact_person_name,
            "contact_person_phone": pl.contact_person_phone,
            "contact_person_email": pl.contact_person_email,
            "alternate_phone": pl.alternate_phone,
            "address": pl.address,
            "landmark": pl.landmark,
            "pincode": pl.pincode,
            "city": pl.city,
            "state": pl.state,
            "country": pl.country,
            "location_type": pl.location_type,
            "active": pl.active,
            "is_default": pl.is_default,
        }

    def to_model(self):
        """Convert to Pydantic model for API responses"""
        from modules.orders.order_schema import Order_Model

        # Build dict with items converted to products format for legacy compatibility
        data = {
            "id": self.id,
            "uuid": self.uuid,
            "order_id": self.order_id,
            "order_date": self.order_date,
            "channel": self.channel,
            "client_id": self.client_id,
            # Consignee
            "consignee_full_name": self.consignee_full_name,
            "consignee_phone": self.consignee_phone,
            "consignee_email": self.consignee_email,
            "consignee_alternate_phone": self.consignee_alternate_phone,
            "consignee_company": self.consignee_company,
            "consignee_gstin": self.consignee_gstin,
            "consignee_address": self.consignee_address,
            "consignee_landmark": self.consignee_landmark,
            "consignee_pincode": self.consignee_pincode,
            "consignee_city": self.consignee_city,
            "consignee_state": self.consignee_state,
            "consignee_country": self.consignee_country,
            # Billing
            "is_billing_same_as_consignee": self.is_billing_same_as_consignee,
            "billing_full_name": self.billing_full_name,
            "billing_phone": self.billing_phone,
            "billing_email": self.billing_email,
            "billing_address": self.billing_address,
            "billing_landmark": self.billing_landmark,
            "billing_pincode": self.billing_pincode,
            "billing_city": self.billing_city,
            "billing_state": self.billing_state,
            "billing_country": self.billing_country,
            # Pickup
            "pickup_location_code": self.pickup_location_code,
            "pickup_location": self._serialize_pickup_location(),
            # Payment
            "payment_mode": self.payment_mode,
            "total_amount": float(self.total_amount or 0),
            "order_value": float(self.order_value or 0),
            "shipping_charges": float(self.shipping_charges or 0),
            "cod_charges": float(self.cod_charges or 0),
            "discount": float(self.discount or 0),
            "gift_wrap_charges": float(self.gift_wrap_charges or 0),
            "other_charges": float(self.other_charges or 0),
            "tax_amount": float(self.tax_amount or 0),
            "cod_to_collect": float(self.cod_to_collect or 0),
            "eway_bill_number": self.eway_bill_number,
            # Package
            "length": float(self.length or 0),
            "breadth": float(self.breadth or 0),
            "height": float(self.height or 0),
            "weight": float(self.weight or 0),
            "applicable_weight": float(self.applicable_weight or 0),
            "volumetric_weight": float(self.volumetric_weight or 0),
            # Shipment
            "aggregator": self.aggregator,
            "courier_partner": self.courier_partner,
            "shipment_mode": self.shipment_mode,
            "awb_number": self.awb_number,
            "shipping_partner_order_id": self.shipping_partner_order_id,
            "shipping_partner_shipping_id": self.shipping_partner_shipping_id,
            "zone": self.zone,
            "manifest_url": self.manifest_url,
            "label_url": self.label_url,
            "invoice_url": self.invoice_url,
            # Status
            "status": self.status,
            "sub_status": self.sub_status,
            "courier_status": self.courier_status,
            "shipment_booking_error": self.shipment_booking_error,
            "pickup_failed_reason": self.pickup_failed_reason,
            "rto_reason": self.rto_reason,
            # Dates
            "booking_date": self.booking_date,
            "pickup_completion_date": self.pickup_completion_date,
            "first_ofp_date": self.first_ofp_date,
            "shipped_date": self.shipped_date,
            "edd": self.edd,
            "first_ofd_date": self.first_ofd_date,
            "delivered_date": self.delivered_date,
            "rto_initiated_date": self.rto_initiated_date,
            "rto_delivered_date": self.rto_delivered_date,
            # Flags
            "is_label_generated": self.is_label_generated,
            "clone_order_count": self.clone_order_count,
            "cancel_count": self.cancel_count,
            "cod_remittance_cycle_id": self.cod_remittance_cycle_id,
            # Timestamps
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            # Products - convert from items relationship to legacy format
            "products": [
                {
                    "name": item.name,
                    "sku_code": item.sku_code or "",
                    "quantity": item.quantity,
                    "unit_price": float(item.unit_price or 0),
                }
                for item in (
                    self.items if hasattr(self, "items") and self.items else []
                )
                if not getattr(item, "is_deleted", False)
            ],
        }
        return Order_Model.model_validate(data)

    @staticmethod
    def create_db_entity(order_data: dict) -> "Order":
        """Create Order entity from dictionary"""
        # Filter out fields that don't exist on the Order model
        filtered_data = {
            k: v for k, v in order_data.items() if k not in ("products", "courier")
        }
        return Order(**filtered_data)

    @classmethod
    def create_new_order(cls, order: "Order", db=None):
        """
        Create a new order in the database.


        """
        try:
            if db is None:
                db = get_db_session()
            db.add(order)
            db.flush()
            return order
        except Exception as e:
            logger.error(msg=f"Error creating order: {str(e)}")
            raise

    @classmethod
    def get_by_order_id(cls, order_id: str, client_id: int, db=None) -> "Order":
        """
        Get order by order_id and client_id.


        """
        if db is None:
            db = get_db_session()
        return (
            db.query(cls)
            .filter(
                cls.order_id == order_id,
                cls.client_id == client_id,
                cls.is_deleted == False,
            )
            .first()
        )

    @classmethod
    def get_by_awb(cls, awb_number: str, db=None) -> "Order":
        """
        Get order by AWB number.


        """
        if db is None:
            db = get_db_session()
        return (
            db.query(cls)
            .filter(cls.awb_number == awb_number, cls.is_deleted == False)
            .first()
        )

    @classmethod
    def exists(cls, order_id: str, client_id: int, db=None) -> bool:
        """
        Check if order exists.

        """
        if db is None:
            db = get_db_session()
        return (
            db.query(cls.id)
            .filter(
                cls.order_id == order_id,
                cls.client_id == client_id,
            )
            .first()
        ) is not None

    def calculate_weights(self):
        """Calculate volumetric and applicable weights (3 decimal places)"""
        self.volumetric_weight = round(
            (float(self.length) * float(self.breadth) * float(self.height)) / 5000,
            3,
        )
        self.applicable_weight = round(
            max(float(self.weight), self.volumetric_weight),
            3,
        )

    # FIX: Cache for product quantity to prevent N+1 queries
    _cached_product_quantity = None

    def get_product_quantity(self, db=None, use_cache=True):
        """
        Get total product quantity from order_item table.


        """
        # Check cache first
        if use_cache and self._cached_product_quantity is not None:
            return self._cached_product_quantity

        # If items relationship is loaded, calculate from it (avoids query)
        if "items" in self.__dict__ and self.items is not None:
            total = sum(
                item.quantity
                for item in self.items
                if not getattr(item, "is_deleted", False)
            )
            self._cached_product_quantity = total
            return total

        # Fallback to database query
        if db is None:
            db = get_db_session()
        from models import OrderItem

        total = (
            db.query(func.sum(OrderItem.quantity))
            .filter(OrderItem.order_id == self.id, OrderItem.is_deleted == False)
            .scalar()
        )
        result = total or 0
        self._cached_product_quantity = result
        return result

    def set_product_quantity_cache(self, quantity: int):
        """
        Manually set the product quantity cache.


        """
        self._cached_product_quantity = quantity

    def clear_product_quantity_cache(self):
        """Clear the cached product quantity, forcing next call to query DB."""
        self._cached_product_quantity = None

    @classmethod
    def prefetch_product_quantities(cls, orders, db=None):
        """
        Batch prefetch product quantities for multiple orders.
        """
        if not orders:
            return {}

        if db is None:
            db = get_db_session()

        from models import OrderItem

        order_ids = [order.id for order in orders]

        # Single query to get all quantities
        quantities = (
            db.query(
                OrderItem.order_id,
                func.sum(OrderItem.quantity).label("total_quantity"),
            )
            .filter(OrderItem.order_id.in_(order_ids), OrderItem.is_deleted == False)
            .group_by(OrderItem.order_id)
            .all()
        )

        # Create mapping
        quantity_map = {row.order_id: row.total_quantity or 0 for row in quantities}

        # Set cache on each order
        for order in orders:
            order.set_product_quantity_cache(quantity_map.get(order.id, 0))

        return quantity_map

    def update_status(self, new_status: str, new_sub_status: str = None):
        """
        Update order status with automatic timestamp tracking.

        Args:
            new_status: New status value
            new_sub_status: New sub_status value (defaults to new_status)
        """
        self.status = new_status
        self.sub_status = new_sub_status or new_status
        self.last_update_date = datetime.now(pytz_timezone("Asia/Kolkata"))

        # Auto-set status dates based on status
        now = datetime.now(pytz_timezone("Asia/Kolkata"))
        status_date_mapping = {
            "booked": "booking_date",
            "picked up": "pickup_completion_date",
            "in transit": "shipped_date",
            "out for delivery": "first_ofd_date",
            "delivered": "delivered_date",
            "rto": "rto_initiated_date",
            "rto_delivered": "rto_delivered_date",
        }

        if new_status.lower() in status_date_mapping:
            date_field = status_date_mapping[new_status.lower()]
            if getattr(self, date_field) is None:
                setattr(self, date_field, now)

    def set_cod_to_collect(self, amount: float = None):
        """
        Set the COD amount to collect.
        For full COD: use total_amount
        For partial COD: specify the amount
        """
        if amount is not None:
            self.cod_to_collect = round(amount, 2)
        elif self.payment_mode and self.payment_mode.lower() == "cod":
            self.cod_to_collect = round(float(self.total_amount or 0), 2)
        else:
            self.cod_to_collect = 0

    # ============================================
    # EAGER LOADING HELPERS (Performance Optimization)
    # ============================================

    @classmethod
    def with_items(cls, query):
        """
        Add eager loading for order items.

        """
        return query.options(selectinload(cls.items))

    @classmethod
    def with_tracking(cls, query):
        """
        Add eager loading for tracking events.

        """
        return query.options(selectinload(cls.tracking_events))

    @classmethod
    def with_audit_logs(cls, query, limit: int = 50):
        """
        Add eager loading for audit logs with limit.

        """
        return query.options(selectinload(cls.audit_logs))

    @classmethod
    def with_full_details(cls, query):
        """
        Add eager loading for all related data (items, tracking, billing).

        """
        return query.options(
            selectinload(cls.items),
            selectinload(cls.tracking_events),
            selectinload(cls.audit_logs),
            joinedload(cls.billing),
            joinedload(cls.pickup_location),
        )

    @classmethod
    def get_with_details(cls, order_id: str, client_id: int, db=None) -> "Order":
        """
        Get order with all related data pre-loaded.

        """
        if db is None:
            db = get_db_session()

        query = db.query(cls).filter(
            cls.order_id == order_id,
            cls.client_id == client_id,
            cls.is_deleted == False,
        )
        query = cls.with_full_details(query)
        return query.first()

    def get_recent_audit_logs(self, db=None, limit: int = 50):
        """
        Get recent audit logs for this order with SQL-level limit.

        """
        if db is None:
            db = get_db_session()

        from models import OrderAuditLog

        return (
            db.query(OrderAuditLog)
            .filter(OrderAuditLog.order_id == self.id)
            .order_by(OrderAuditLog.timestamp.desc())
            .limit(limit)
            .all()
        )
