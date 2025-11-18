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
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON, JSONB

from pytz import timezone

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class Order(DBBase, DBBaseClass):

    __tablename__ = "order"

    # order details
    order_id = Column(String(255), nullable=False)
    order_type = Column(String(10), nullable=False)
    order_date = Column(TIMESTAMP(timezone=True), nullable=False)

    channel = Column(String(100), nullable=True)

    company_id = Column(Integer, ForeignKey("company.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    company = relationship("Company", back_populates="orders", lazy="noload")
    client = relationship("Client", back_populates="orders", lazy="noload")
    pickup_location = relationship("Pickup_Location", lazy="noload")

    order_tags = relationship("Order_Tags", back_populates="order", lazy="noload")

    # Relation with Discrepancie
    rate_discrepancies = relationship("Admin_Rate_Discrepancie", back_populates="order")

    # shipping details
    consignee_full_name = Column(String(100), nullable=False)
    consignee_phone = Column(String(15), nullable=False)
    consignee_alternate_phone = Column(String(15), nullable=True)  # optional
    consignee_email = Column(String(150), nullable=True)  # optional
    consignee_company = Column(String(100), nullable=True)  # optional
    consignee_gstin = Column(String(15), nullable=True)  # optional
    consignee_address = Column(String(255), nullable=False)
    consignee_landmark = Column(String(255), nullable=True)  # optional
    consignee_pincode = Column(String(10), nullable=False)
    consignee_city = Column(String(100), nullable=False)
    consignee_state = Column(String(100), nullable=False)
    consignee_country = Column(String(100), nullable=False)

    # billing details
    billing_is_same_as_consignee = Column(Boolean, nullable=True)
    billing_full_name = Column(String(100), nullable=True)
    billing_phone = Column(String(15), nullable=True)
    billing_email = Column(String(150), nullable=True)  # optional
    billing_address = Column(String(255), nullable=True)
    billing_landmark = Column(String(255), nullable=True)
    billing_pincode = Column(String(10), nullable=True)
    billing_city = Column(String(100), nullable=True)
    billing_state = Column(String(100), nullable=True)
    billing_country = Column(String(100), nullable=True)

    # pickup details
    pickup_location_code = Column(
        String(255), ForeignKey("pickup_location.location_code"), nullable=False
    )

    # payment
    payment_mode = Column(String(15), nullable=False)
    total_amount = Column(Numeric(10, 3), nullable=False)
    order_value = Column(Numeric(10, 3), nullable=False)
    shipping_charges = Column(Numeric(10, 3), nullable=True)  # optional
    cod_charges = Column(Numeric(10, 3), nullable=True)  # optional
    discount = Column(Numeric(10, 3), nullable=True)  # optional
    gift_wrap_charges = Column(Numeric(10, 3), nullable=True)  # optional
    other_charges = Column(Numeric(10, 3), nullable=True)  # optional
    tax_amount = Column(Numeric(10, 3), nullable=True)  # optional

    # invoice details
    invoice_number = Column(String(100), nullable=True)  # optional
    invoice_date = Column(String(30), nullable=True)  # optional
    invoice_amount = Column(Numeric(10, 3), nullable=True)  # optional
    eway_bill_number = Column(String(100), nullable=True)  # optional

    # products
    products = Column(JSONB, nullable=False)
    product_quantity = Column(Integer, nullable=False, default=1)

    # package
    length = Column(Numeric(10, 3), nullable=False)
    breadth = Column(Numeric(10, 3), nullable=False)
    height = Column(Numeric(10, 3), nullable=False)
    weight = Column(Numeric(10, 3), nullable=False)
    applicable_weight = Column(Numeric(10, 3), nullable=False)
    volumetric_weight = Column(Numeric(10, 3), nullable=False)

    # courier
    aggregator = Column(String(255), nullable=True)
    courier_partner = Column(String(255), nullable=True)
    shipment_mode = Column(String(255), nullable=True)
    awb_number = Column(String(255), nullable=True)
    shipping_partner_order_id = Column(String(255), nullable=True)
    shipping_partner_shipping_id = Column(String(255), nullable=True)

    # shipment
    zone = Column(String(1), nullable=True)

    manifest_url = Column(String(255), nullable=True)  # optional
    label_url = Column(String(255), nullable=True)  # optional
    invoice_url = Column(String(255), nullable=True)  # optional
    tracking_id = Column(String(255), nullable=True)  # optional

    status = Column(String(255), nullable=False)
    sub_status = Column(String(255), nullable=False)
    courier_status = Column(String(255), nullable=True)

    tracking_info = Column(JSON, nullable=True)
    tracking_response = Column(JSON, nullable=True)

    action_history = Column(JSON, nullable=True)

    forward_freight = Column(Numeric(10, 3), nullable=True)
    forward_cod_charge = Column(Numeric(10, 3), nullable=True)
    forward_tax = Column(Numeric(10, 3), nullable=True)

    rto_freight = Column(Numeric(10, 3), nullable=True)
    rto_tax = Column(Numeric(10, 3), nullable=True)

    buy_forward_freight = Column(Numeric(10, 3), nullable=True)
    buy_forward_cod_charge = Column(Numeric(10, 3), nullable=True)
    buy_forward_tax = Column(Numeric(10, 3), nullable=True)

    buy_rto_freight = Column(Numeric(10, 3), nullable=True)
    buy_rto_tax = Column(Numeric(10, 3), nullable=True)

    request = Column(JSON, nullable=True)

    source = Column(String(255), nullable=True)
    marketplace_order_id = Column(String(255), nullable=True)

    cod_remittance_cycle_id = Column(
        Integer, ForeignKey("cod_remittance.id"), nullable=True
    )

    clone_order_count = Column(Integer, nullable=False, default=0)
    cancel_count = Column(Integer, nullable=False, default=0)

    store_id = Column(Integer, nullable=True)
    invoice_id = Column(Integer, nullable=True)

    shipment_booking_error = Column(String(255), nullable=True)  # optional

    # additional details
    pickup_failed_reason = Column(String(255), nullable=True)  # optional
    rto_reason = Column(String(255), nullable=True)  # optional

    # status dates
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
        default=datetime.now(timezone("Asia/Kolkata")),
    )

    # flags
    is_label_generated = Column(Boolean, nullable=False, default=False)

    order_tags = Column(JSON, nullable=True, default=[])

    # Relationship to the Ndr table (one-to-many)
    ndr = relationship("Ndr", back_populates="order")

    # Relationship to the CourierBilling table (one-to-many)
    courier_billing = relationship("CourierBilling", back_populates="order")

    __table_args__ = (
        UniqueConstraint("order_id", "client_id", name="unique_order_id_for_client"),
    )

    def to_model(self):

        from modules.orders.order_schema import Order_Model

        return Order_Model.model_validate(self)

    def create_db_entity(OrderRequest):
        return Order(**OrderRequest)

    @classmethod
    def create_new_order(cls, order):
        try:
            db = get_db_session()
            db.add(order)
            db.flush()
            db.commit()

            return order

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                msg="Unhandled error: {}".format(str(e)),
            )

    @classmethod
    def get_order_by_id(cls, id):
        user = super().get_by_id(id)
        return user if user else None

    @classmethod
    def get_order_by_uuid(cls, uuid):
        user = super().get_by_uuid(uuid)
        return user if user else None
