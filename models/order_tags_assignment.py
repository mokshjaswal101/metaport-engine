"""
Order Tags Assignment Model
Junction table for many-to-many relationship between orders and tags.
"""

from datetime import datetime
from sqlalchemy import Column, String, Integer, ForeignKey, TIMESTAMP, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from pytz import timezone

from database import DBBaseClass, DBBase


class OrderTagsAssignment(DBBase, DBBaseClass):
    """
    Junction table linking orders to tags.
    
    Design: Uses auto-generated id as primary key with unique constraint
    on (order_id, tag_id) to prevent duplicates while allowing soft deletes.
    """

    __tablename__ = "order_tags_assignment"

    # Foreign keys
    order_id = Column(
        Integer,
        ForeignKey("order.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    tag_id = Column(
        Integer,
        ForeignKey("order_tags.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    
    # When the tag was assigned
    assigned_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone("Asia/Kolkata")),
    )

    # Relationships
    order = relationship("Order", lazy="noload")
    tag = relationship("OrderTags", lazy="noload")

    __table_args__ = (
        # Prevent duplicate tag assignments (respects soft delete)
        UniqueConstraint(
            "order_id", "tag_id",
            name="uq_order_tag_assignment",
        ),
        # Index for finding all tags for an order
        Index("ix_order_tags_assignment_order", "order_id"),
        # Index for finding all orders with a tag
        Index("ix_order_tags_assignment_tag", "tag_id"),
    )

    def to_model(self):
        from modules.order_tags.order_tags_schema import OrderTagsAssignmentModel
        return OrderTagsAssignmentModel.model_validate(self)

    @staticmethod
    def create_db_entity(data):
        return OrderTagsAssignment(**data)
