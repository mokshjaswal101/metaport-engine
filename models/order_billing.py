"""
Order Billing Model
Stores freight and financial data for each order.
Separated from the main order table for better query performance.

All price fields use Numeric(10, 2) for 2 decimal places.
"""

from decimal import Decimal
from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    Numeric,
    Index,
)
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class OrderBilling(DBBase, DBBaseClass):
    """
    Stores freight charges and financial data for an order.

    This is a one-to-one relationship with Order.
    Separated because:
    - These fields are not needed for order list display
    - These fields are primarily used for invoicing/reports
    - Reduces main order table size for faster queries

    Contains both:
    - Sell rates (charged to client)
    - Buy rates (paid to courier)

    Relationships:
    - One OrderBilling belongs to one Order (one-to-one)

    Performance Considerations:
    - Unique constraint on order_id ensures one-to-one
    - Only fetched when billing/invoice data is needed
    """

    __tablename__ = "order_billing"

    # Foreign key to order (unique for one-to-one)
    order_id = Column(
        Integer,
        ForeignKey("order.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    # ============================================
    # SELL RATES (charged to client) - 2 decimal places
    # ============================================

    # Forward journey charges
    forward_freight = Column(Numeric(10, 2), nullable=True, default=0)
    forward_cod_charge = Column(Numeric(10, 2), nullable=True, default=0)
    forward_tax = Column(Numeric(10, 2), nullable=True, default=0)

    # RTO charges (if applicable)
    rto_freight = Column(Numeric(10, 2), nullable=True, default=0)
    rto_tax = Column(Numeric(10, 2), nullable=True, default=0)

    # COD charge reversed on RTO (refunded to client)
    cod_charge_reversed = Column(Numeric(10, 2), nullable=True, default=0)

    # ============================================
    # BUY RATES (paid to courier/aggregator) - 2 decimal places
    # ============================================

    # Forward journey buy rates
    buy_forward_freight = Column(Numeric(10, 2), nullable=True, default=0)
    buy_forward_cod_charge = Column(Numeric(10, 2), nullable=True, default=0)
    buy_forward_tax = Column(Numeric(10, 2), nullable=True, default=0)

    # RTO buy rates
    buy_rto_freight = Column(Numeric(10, 2), nullable=True, default=0)
    buy_rto_tax = Column(Numeric(10, 2), nullable=True, default=0)

    # Relationship back to order
    order = relationship("Order", back_populates="billing", lazy="noload", uselist=False)

    # Indexes
    __table_args__ = (
        # Primary lookup by order
        Index("ix_order_billing_v2_order", "order_id", unique=True),
    )

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "forward_freight": round(float(self.forward_freight or 0), 2),
            "forward_cod_charge": round(float(self.forward_cod_charge or 0), 2),
            "forward_tax": round(float(self.forward_tax or 0), 2),
            "rto_freight": round(float(self.rto_freight or 0), 2),
            "rto_tax": round(float(self.rto_tax or 0), 2),
            "cod_charge_reversed": round(float(self.cod_charge_reversed or 0), 2),
            "buy_forward_freight": round(float(self.buy_forward_freight or 0), 2),
            "buy_forward_cod_charge": round(float(self.buy_forward_cod_charge or 0), 2),
            "buy_forward_tax": round(float(self.buy_forward_tax or 0), 2),
            "buy_rto_freight": round(float(self.buy_rto_freight or 0), 2),
            "buy_rto_tax": round(float(self.buy_rto_tax or 0), 2),
        }

    @property
    def total_sell_freight(self) -> Decimal:
        """Calculate total sell freight (forward + RTO if applicable)"""
        forward = (self.forward_freight or 0) + (self.forward_cod_charge or 0) + (self.forward_tax or 0)
        rto = (self.rto_freight or 0) + (self.rto_tax or 0)
        return Decimal(str(forward + rto))

    @property
    def total_buy_freight(self) -> Decimal:
        """Calculate total buy freight (forward + RTO if applicable)"""
        forward = (self.buy_forward_freight or 0) + (self.buy_forward_cod_charge or 0) + (self.buy_forward_tax or 0)
        rto = (self.buy_rto_freight or 0) + (self.buy_rto_tax or 0)
        return Decimal(str(forward + rto))

    @property
    def margin(self) -> Decimal:
        """Calculate profit margin (sell - buy)"""
        return self.total_sell_freight - self.total_buy_freight

    @classmethod
    def create_for_order(cls, order_id: int) -> "OrderBilling":
        """
        Create an empty billing record for an order.
        Called when order is created. Rates populated later.
        
        Args:
            order_id: The order this billing belongs to
            
        Returns:
            OrderBilling instance (not yet added to session)
        """
        return cls(order_id=order_id)

    @classmethod
    def get_or_create(cls, order_id: int, db) -> "OrderBilling":
        """
        Get existing billing record or create new one.
        
        Args:
            order_id: The order to get/create billing for
            db: Database session
            
        Returns:
            OrderBilling instance
        """
        billing = db.query(cls).filter(cls.order_id == order_id).first()
        if not billing:
            billing = cls(order_id=order_id)
            db.add(billing)
            db.flush()
        return billing

    def update_sell_rates(
        self,
        forward_freight: float = None,
        forward_cod_charge: float = None,
        forward_tax: float = None,
        rto_freight: float = None,
        rto_tax: float = None,
    ):
        """Update sell rates"""
        if forward_freight is not None:
            self.forward_freight = Decimal(str(forward_freight))
        if forward_cod_charge is not None:
            self.forward_cod_charge = Decimal(str(forward_cod_charge))
        if forward_tax is not None:
            self.forward_tax = Decimal(str(forward_tax))
        if rto_freight is not None:
            self.rto_freight = Decimal(str(rto_freight))
        if rto_tax is not None:
            self.rto_tax = Decimal(str(rto_tax))

    def update_buy_rates(
        self,
        buy_forward_freight: float = None,
        buy_forward_cod_charge: float = None,
        buy_forward_tax: float = None,
        buy_rto_freight: float = None,
        buy_rto_tax: float = None,
    ):
        """Update buy rates"""
        if buy_forward_freight is not None:
            self.buy_forward_freight = Decimal(str(buy_forward_freight))
        if buy_forward_cod_charge is not None:
            self.buy_forward_cod_charge = Decimal(str(buy_forward_cod_charge))
        if buy_forward_tax is not None:
            self.buy_forward_tax = Decimal(str(buy_forward_tax))
        if buy_rto_freight is not None:
            self.buy_rto_freight = Decimal(str(buy_rto_freight))
        if buy_rto_tax is not None:
            self.buy_rto_tax = Decimal(str(buy_rto_tax))

