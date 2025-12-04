"""
Order Tracking Model
Stores shipment tracking events for each order.
Normalized from the tracking_info JSONB column in the order table.
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    TIMESTAMP,
    Index,
    text,
)
from sqlalchemy.orm import relationship
from pytz import timezone

from database import DBBaseClass, DBBase


class OrderTracking(DBBase, DBBaseClass):
    """
    Represents a single tracking event for an order shipment.
    
    Each order can have multiple tracking events as the shipment
    progresses through various stages (booked, picked up, in transit,
    out for delivery, delivered, RTO, etc.)
    
    Relationships:
    - Many OrderTracking events belong to one Order
    
    Performance Considerations:
    - Indexed on order_id for fast order lookups
    - Indexed on status for status-based queries
    - Indexed on event_datetime for chronological ordering
    - High volume table - consider partitioning by date for very large datasets
    """

    __tablename__ = "order_tracking"

    # Foreign key to order
    order_id = Column(
        Integer,
        ForeignKey("order.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Tracking event details
    status = Column(String(100), nullable=False, index=True)
    description = Column(String(500), nullable=True)
    sub_info = Column(String(255), nullable=True)
    location = Column(String(255), nullable=True)
    
    # When this tracking event occurred (from courier)
    event_datetime = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone("Asia/Kolkata")),
    )

    # Raw status from courier (before mapping)
    courier_status = Column(String(100), nullable=True)

    # Relationship back to order
    order = relationship("Order", back_populates="tracking_events", lazy="noload")

    # Indexes for common query patterns
    __table_args__ = (
        # Primary lookup - all events for an order, chronologically
        Index("ix_order_tracking_v2_order_dt", "order_id", "event_datetime"),
        # Status-based queries
        Index("ix_order_tracking_v2_status", "status"),
        # Date-based queries for analytics
        Index("ix_order_tracking_v2_datetime", "event_datetime"),
        # Composite for order + status
        Index("ix_order_tracking_v2_order_status", "order_id", "status"),
        # Deduplication constraint: prevent duplicate tracking events
        # Same order + status + event_datetime = duplicate
        Index(
            "ix_order_tracking_v2_dedup",
            "order_id", "status", "event_datetime",
            unique=True,
            postgresql_where=text("is_deleted = false"),
        ),
    )

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "status": self.status,
            "description": self.description,
            "subinfo": self.sub_info,
            "location": self.location,
            "datetime": self.event_datetime.strftime("%d-%m-%Y %H:%M:%S") if self.event_datetime else None,
            "courier_status": self.courier_status,
        }

    @classmethod
    def create_from_tracking_dict(cls, order_id: int, tracking: dict) -> "OrderTracking":
        """
        Factory method to create OrderTracking from tracking dictionary.
        Used when processing webhook updates from couriers.
        
        Args:
            order_id: The order this tracking event belongs to
            tracking: Dictionary with status, description, subinfo, datetime, location
            
        Returns:
            OrderTracking instance (not yet added to session)
        """
        # Parse datetime from various formats
        event_dt = tracking.get("datetime")
        if isinstance(event_dt, str):
            # Try common formats
            for fmt in ["%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M"]:
                try:
                    event_dt = datetime.strptime(event_dt, fmt)
                    # Localize to IST if naive
                    if event_dt.tzinfo is None:
                        event_dt = timezone("Asia/Kolkata").localize(event_dt)
                    break
                except ValueError:
                    continue
            else:
                # Default to now if parsing fails
                event_dt = datetime.now(timezone("Asia/Kolkata"))
        elif event_dt is None:
            event_dt = datetime.now(timezone("Asia/Kolkata"))

        return cls(
            order_id=order_id,
            status=str(tracking.get("status", "")).strip()[:100],
            description=str(tracking.get("description", "")).strip()[:500] if tracking.get("description") else None,
            sub_info=str(tracking.get("subinfo", "")).strip()[:255] if tracking.get("subinfo") else None,
            location=str(tracking.get("location", "")).strip()[:255] if tracking.get("location") else None,
            event_datetime=event_dt,
            courier_status=str(tracking.get("courier_status", "")).strip()[:100] if tracking.get("courier_status") else None,
        )

    @staticmethod
    def bulk_create_from_tracking_list(order_id: int, tracking_list: list) -> list:
        """
        Create multiple OrderTracking instances from a list of tracking dicts.
        Optimized for bulk tracking updates.
        
        Args:
            order_id: The order these tracking events belong to
            tracking_list: List of tracking event dictionaries
            
        Returns:
            List of OrderTracking instances
        """
        events = []
        for tracking in tracking_list:
            if tracking and tracking.get("status"):
                events.append(OrderTracking.create_from_tracking_dict(order_id, tracking))
        return events

    @classmethod
    def get_latest_for_order(cls, order_id: int, db):
        """
        Get the most recent tracking event for an order.
        
        Args:
            order_id: The order to get tracking for
            db: Database session
            
        Returns:
            OrderTracking instance or None
        """
        return (
            db.query(cls)
            .filter(cls.order_id == order_id, cls.is_deleted == False)
            .order_by(cls.event_datetime.desc())
            .first()
        )

