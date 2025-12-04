"""
Order Audit Log Model
Stores action history for each order.
Normalized from the action_history JSONB column in the order table.

This is separate from the global activity_log table due to the 
massive volume of order-related actions at scale.
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    TIMESTAMP,
    Index,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from pytz import timezone

from database import DBBaseClass, DBBase


class OrderAuditLog(DBBase, DBBaseClass):
    """
    Represents a single audit log entry for an order.
    
    Tracks all actions performed on an order:
    - Order created
    - Order updated
    - AWB assigned
    - Status changed
    - Pickup scheduled
    - Cancelled
    - etc.
    
    Relationships:
    - Many OrderAuditLog entries belong to one Order
    
    Performance Considerations:
    - Indexed on order_id for fast order lookups
    - Indexed on action for action-type queries
    - Indexed on timestamp for chronological ordering
    - Very high volume table - consider partitioning by timestamp
    """

    __tablename__ = "order_audit_log"

    # Foreign key to order
    order_id = Column(
        Integer,
        ForeignKey("order.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Action details
    action = Column(String(100), nullable=False, index=True)
    message = Column(String(500), nullable=False)
    
    # Who performed the action
    user_id = Column(Integer, nullable=True, index=True)
    user_name = Column(String(100), nullable=True)  # Denormalized for display
    
    # When the action occurred
    timestamp = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone("Asia/Kolkata")),
        index=True,
    )

    # Additional context (optional)
    # Can store: old values, new values, API response, etc.
    # Note: Using 'extra_data' instead of 'metadata' (reserved by SQLAlchemy)
    extra_data = Column(JSONB, nullable=True)

    # Relationship back to order
    order = relationship("Order", back_populates="audit_logs", lazy="noload")

    # Indexes for common query patterns
    __table_args__ = (
        # Primary lookup - all logs for an order, chronologically
        Index("ix_order_audit_v2_order_ts", "order_id", "timestamp"),
        # Action-based queries
        Index("ix_order_audit_v2_action", "action"),
        # User-based queries (who did what)
        Index("ix_order_audit_v2_user", "user_id"),
        # Date-based queries for analytics
        Index("ix_order_audit_v2_timestamp", "timestamp"),
        # Composite for order + action
        Index("ix_order_audit_v2_order_action", "order_id", "action"),
    )

    # Common action types
    ACTION_CREATED = "created"
    ACTION_UPDATED = "updated"
    ACTION_AWB_ASSIGNED = "awb_assigned"
    ACTION_STATUS_CHANGED = "status_changed"
    ACTION_PICKUP_SCHEDULED = "pickup_scheduled"
    ACTION_PICKED_UP = "picked_up"
    ACTION_IN_TRANSIT = "in_transit"
    ACTION_OUT_FOR_DELIVERY = "out_for_delivery"
    ACTION_DELIVERED = "delivered"
    ACTION_RTO_INITIATED = "rto_initiated"
    ACTION_RTO_DELIVERED = "rto_delivered"
    ACTION_CANCELLED = "cancelled"
    ACTION_NDR_RAISED = "ndr_raised"
    ACTION_NDR_ACTION = "ndr_action"
    ACTION_LABEL_GENERATED = "label_generated"
    ACTION_DIMENSIONS_UPDATED = "dimensions_updated"
    ACTION_PICKUP_LOCATION_CHANGED = "pickup_location_changed"
    ACTION_CLONED = "cloned"

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "id": self.id,
            "action": self.action,
            "message": self.message,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M") if self.timestamp else None,
            "extra_data": self.extra_data,
        }

    @classmethod
    def create_log(
        cls,
        order_id: int,
        action: str,
        message: str,
        user_id: int = None,
        user_name: str = None,
        extra_data: dict = None,
    ) -> "OrderAuditLog":
        """
        Factory method to create an audit log entry.
        
        Args:
            order_id: The order this log belongs to
            action: Action type (use class constants)
            message: Human-readable description
            user_id: ID of user who performed the action
            user_name: Name of user (denormalized)
            extra_data: Additional context as dict
            
        Returns:
            OrderAuditLog instance (not yet added to session)
        """
        return cls(
            order_id=order_id,
            action=str(action).strip()[:100],
            message=str(message).strip()[:500],
            user_id=user_id,
            user_name=str(user_name).strip()[:100] if user_name else None,
            timestamp=datetime.now(timezone("Asia/Kolkata")),
            extra_data=extra_data,
        )

    @classmethod
    def log_order_created(cls, order_id: int, user_id: int = None, user_name: str = None, source: str = "platform") -> "OrderAuditLog":
        """Log order creation"""
        return cls.create_log(
            order_id=order_id,
            action=cls.ACTION_CREATED,
            message=f"Order created on {source}",
            user_id=user_id,
            user_name=user_name,
            extra_data={"source": source},
        )

    @classmethod
    def log_awb_assigned(cls, order_id: int, awb_number: str, courier: str, user_id: int = None, user_name: str = None) -> "OrderAuditLog":
        """Log AWB assignment"""
        return cls.create_log(
            order_id=order_id,
            action=cls.ACTION_AWB_ASSIGNED,
            message=f"AWB {awb_number} assigned via {courier}",
            user_id=user_id,
            user_name=user_name,
            extra_data={"awb_number": awb_number, "courier": courier},
        )

    @classmethod
    def log_status_change(cls, order_id: int, old_status: str, new_status: str, user_id: int = None, user_name: str = None) -> "OrderAuditLog":
        """Log status change"""
        return cls.create_log(
            order_id=order_id,
            action=cls.ACTION_STATUS_CHANGED,
            message=f"Status changed from {old_status} to {new_status}",
            user_id=user_id,
            user_name=user_name,
            extra_data={"old_status": old_status, "new_status": new_status},
        )

    @classmethod
    def log_order_cancelled(cls, order_id: int, reason: str = None, user_id: int = None, user_name: str = None) -> "OrderAuditLog":
        """Log order cancellation"""
        return cls.create_log(
            order_id=order_id,
            action=cls.ACTION_CANCELLED,
            message=f"Order cancelled{': ' + reason if reason else ''}",
            user_id=user_id,
            user_name=user_name,
            extra_data={"reason": reason} if reason else None,
        )

    @staticmethod
    def bulk_create_from_history(order_id: int, history_list: list) -> list:
        """
        Create multiple OrderAuditLog instances from legacy action_history format.
        Used for data migration from old JSONB format.
        
        Args:
            order_id: The order these logs belong to
            history_list: List of {timestamp, message, user_data} dicts
            
        Returns:
            List of OrderAuditLog instances
        """
        logs = []
        for entry in history_list:
            if entry and entry.get("message"):
                # Parse timestamp
                ts = entry.get("timestamp")
                if isinstance(ts, str):
                    try:
                        ts = datetime.strptime(ts, "%Y-%m-%d %H:%M")
                        ts = timezone("Asia/Kolkata").localize(ts)
                    except ValueError:
                        ts = datetime.now(timezone("Asia/Kolkata"))
                elif ts is None:
                    ts = datetime.now(timezone("Asia/Kolkata"))

                log = OrderAuditLog(
                    order_id=order_id,
                    action=OrderAuditLog.ACTION_UPDATED,  # Default for legacy
                    message=str(entry.get("message", ""))[:500],
                    user_id=entry.get("user_data") if isinstance(entry.get("user_data"), int) else None,
                    timestamp=ts,
                )
                logs.append(log)
        return logs

    @classmethod
    def get_for_order(cls, order_id: int, db, limit: int = 50):
        """
        Get audit logs for an order, most recent first.
        
        Args:
            order_id: The order to get logs for
            db: Database session
            limit: Maximum number of logs to return
            
        Returns:
            List of OrderAuditLog instances
        """
        return (
            db.query(cls)
            .filter(cls.order_id == order_id, cls.is_deleted == False)
            .order_by(cls.timestamp.desc())
            .limit(limit)
            .all()
        )

