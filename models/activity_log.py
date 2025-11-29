from sqlalchemy import Column, String, Integer, Text, Index
from sqlalchemy.dialects.postgresql import JSON
from database.db import DBBaseClass, DBBase


class ActivityLog(DBBase, DBBaseClass):
    """
    Activity log for tracking all entity changes across the system.
    Used for auditing, debugging, and compliance purposes.
    """

    __tablename__ = "activity_log"

    # Entity information
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(String(255), nullable=False)

    # Action details
    action = Column(String(50), nullable=False)

    # User who performed the action
    user_id = Column(Integer, nullable=True)
    user_email = Column(String(255), nullable=True)

    # Client context
    client_id = Column(Integer, nullable=False)

    # State changes
    old_value = Column(JSON, nullable=True)  # Previous state (null for CREATE)
    new_value = Column(JSON, nullable=True)  # New state (null for DELETE)

    # Description of the change
    description = Column(Text, nullable=True)

    # Request metadata
    ip_address = Column(String(45), nullable=True)  # IPv6 support
    user_agent = Column(Text, nullable=True)
    endpoint = Column(String(255), nullable=True)

    # Additional context
    extra_data = Column(JSON, nullable=True)  # Any additional context

    # Composite indexes for efficient queries
    __table_args__ = (
        # Index for entity history lookups
        Index("ix_activity_log_entity", "entity_type", "entity_id"),
        # Index for client activity lookups
        Index("ix_activity_log_client", "client_id", "entity_type"),
        # Index for user activity lookups
        Index("ix_activity_log_user", "user_id"),
        # Index for action filtering
        Index("ix_activity_log_action_type", "action"),
    )

    def __repr__(self):
        return f"<ActivityLog(id={self.id}, entity_type={self.entity_type}, entity_id={self.entity_id}, action={self.action})>"

    @classmethod
    def log_activity(
        cls,
        entity_type: str,
        entity_id: str,
        action: str,
        client_id: int,
        user_id: int = None,
        user_email: str = None,
        old_value: dict = None,
        new_value: dict = None,
        description: str = None,
        ip_address: str = None,
        user_agent: str = None,
        endpoint: str = None,
        extra_data: dict = None,
    ):
        """Create a new activity log entry"""
        from context_manager.context import get_db_session

        log_data = {
            "entity_type": entity_type,
            "entity_id": str(entity_id),
            "action": action,
            "client_id": client_id,
            "user_id": user_id,
            "user_email": user_email,
            "old_value": old_value,
            "new_value": new_value,
            "description": description,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "endpoint": endpoint,
            "extra_data": extra_data,
        }

        with get_db_session() as db:
            activity_log = cls(**log_data)
            db.add(activity_log)
            db.flush()
            return activity_log

    @classmethod
    def get_entity_history(cls, entity_type: str, entity_id: str, limit: int = 100):
        """Get activity history for a specific entity"""
        from context_manager.context import get_db_session

        with get_db_session() as db:
            return (
                db.query(cls)
                .filter(
                    cls.entity_type == entity_type,
                    cls.entity_id == str(entity_id),
                    cls.is_deleted.is_(False),
                )
                .order_by(cls.created_at.desc())
                .limit(limit)
                .all()
            )

    @classmethod
    def get_client_activity(
        cls, client_id: int, entity_type: str = None, limit: int = 100
    ):
        """Get activity logs for a specific client"""
        from context_manager.context import get_db_session

        with get_db_session() as db:
            query = db.query(cls).filter(
                cls.client_id == client_id,
                cls.is_deleted.is_(False),
            )

            if entity_type:
                query = query.filter(cls.entity_type == entity_type)

            return query.order_by(cls.created_at.desc()).limit(limit).all()

    @classmethod
    def get_user_activity(cls, user_id: int, limit: int = 100):
        """Get activity logs for a specific user"""
        from context_manager.context import get_db_session

        with get_db_session() as db:
            return (
                db.query(cls)
                .filter(
                    cls.user_id == user_id,
                    cls.is_deleted.is_(False),
                )
                .order_by(cls.created_at.desc())
                .limit(limit)
                .all()
            )
