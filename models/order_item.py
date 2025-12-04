"""
Order Item Model
Stores individual product line items for each order.
Normalized from the products JSONB column in the order table.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    Numeric,
    Index,
)
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class OrderItem(DBBase, DBBaseClass):
    """
    Represents a single product line item within an order.

    Relationships:
    - Many OrderItems belong to one Order
    - Future: Links to Product catalog via product_id

    Performance Considerations:
    - Indexed on order_id for fast order lookups
    - Indexed on sku_code for SKU-based searches
    - Indexed on product_id for future catalog integration
    """

    __tablename__ = "order_item"

    # Foreign key to order
    order_id = Column(
        Integer,
        ForeignKey("order.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Product details
    name = Column(String(255), nullable=False)
    sku_code = Column(String(100), nullable=True)
    quantity = Column(Integer, nullable=False, default=1)
    unit_price = Column(Numeric(10, 2), nullable=False, default=0)  # 2 decimal places for price

    # Future: Link to product catalog
    # When product catalog is implemented, this will be a FK
    product_id = Column(Integer, nullable=True)

    # Relationship back to order
    order = relationship("Order", back_populates="items", lazy="noload")

    # Composite indexes for common query patterns
    __table_args__ = (
        # Index for fetching all items for an order (most common)
        Index("ix_order_item_v2_order", "order_id"),
        # Index for SKU-based filtering across orders
        Index("ix_order_item_v2_sku", "sku_code"),
        # Index for product catalog lookups (future)
        Index("ix_order_item_v2_product", "product_id"),
        # Composite for order + sku queries
        Index("ix_order_item_v2_order_sku", "order_id", "sku_code"),
    )

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "name": self.name,
            "sku_code": self.sku_code,
            "quantity": self.quantity,
            "unit_price": round(float(self.unit_price or 0), 2),
            "product_id": self.product_id,
        }

    @classmethod
    def create_from_product_dict(cls, order_id: int, product: dict) -> "OrderItem":
        """
        Factory method to create OrderItem from product dictionary.
        Used during order creation and bulk import.

        Args:
            order_id: The order this item belongs to
            product: Dictionary with name, sku_code, quantity, unit_price

        Returns:
            OrderItem instance (not yet added to session)
        """
        return cls(
            order_id=order_id,
            name=str(product.get("name", "")).strip()[:255],
            sku_code=(
                str(product.get("sku_code", "")).strip()[:100]
                if product.get("sku_code")
                else None
            ),
            quantity=int(product.get("quantity", 1)),
            unit_price=float(product.get("unit_price", 0)),
            product_id=product.get("product_id"),
        )

    @staticmethod
    def bulk_create_from_products(order_id: int, products: list) -> list:
        """
        Create multiple OrderItem instances from a list of product dicts.
        Optimized for bulk order creation.

        Args:
            order_id: The order these items belong to
            products: List of product dictionaries

        Returns:
            List of OrderItem instances
        """
        items = []
        for product in products:
            if product and product.get("name"):
                items.append(OrderItem.create_from_product_dict(order_id, product))
        return items
