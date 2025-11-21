"""
Integration Audit Log Model
Tracks all integration lifecycle events:
- OAuth events (connect, disconnect, reconnect)
- Integration management (pause, resume, delete)
- Store actions (uninstall from Shopify)
- Test connections
- Configuration changes
- User actions

Lower volume, longer retention, audit/compliance focused
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    Text,
    JSON,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import relationship
from datetime import datetime
from database import DBBaseClass, DBBase
from logger import logger


class IntegrationAuditLog(DBBase, DBBaseClass):
    __tablename__ = "integration_audit_logs"

    # Integration reference
    integration_id = Column(
        Integer, ForeignKey("channel_client_integrations.id"), nullable=False, index=True
    )

    # Client tracking (which user performed the action)
    client_id = Column(
        Integer, ForeignKey("client.id"), nullable=True, index=True
    )  # User/client who performed the action (null for system/webhook events)

    # Event classification
    event_type = Column(
        String(50), nullable=False, index=True
    )  # 'integration_connected', 'integration_disconnected', 'integration_reconnected',
       # 'integration_paused', 'integration_resumed', 'integration_deleted',
       # 'store_uninstalled', 'test_connection', 'webhook_registered', 'config_updated', etc.
    
    trigger = Column(
        String(50), nullable=False, index=True
    )  # 'user_action', 'store_action', 'webhook', 'system', 'api'

    # Status
    status = Column(
        String(50), nullable=False, index=True
    )  # 'success', 'failed'

    # Event metadata (flexible JSON)
    event_data = Column(JSON, nullable=True)  # Shop domain, shop name, user details, config changes, etc.
    error_details = Column(JSON, nullable=True)  # Detailed error information

    # Timing
    occurred_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)

    # Error information
    error_message = Column(Text, nullable=True)
    error_code = Column(String(50), nullable=True)

    # IP address / request metadata (for audit trail)
    ip_address = Column(String(45), nullable=True)  # IPv4 or IPv6
    user_agent = Column(String(500), nullable=True)

    # Relationships (lazy="noload" prevents automatic loading, avoiding FK validation errors)
    integration = relationship(
        "ClientChannelIntegration", lazy="noload"
    )
    client = relationship(
        "Client", 
        lazy="noload"
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            "id": self.id,
            "integration_id": self.integration_id,
            "client_id": self.client_id,
            "event_type": self.event_type,
            "trigger": self.trigger,
            "status": self.status,
            "event_data": self.event_data,
            "error_details": self.error_details,
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
            "error_message": self.error_message,
            "error_code": self.error_code,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def create_audit_log(
        cls,
        db_session,
        integration_id: int,
        event_type: str,
        trigger: str,
        status: str,
        client_id: int = None,
        event_data: dict = None,
        error_message: str = None,
        error_code: str = None,
        error_details: dict = None,
        ip_address: str = None,
        user_agent: str = None,
        occurred_at: datetime = None,
    ):
        """
        Create an audit log entry for integration events
        
        Args:
            db_session: Database session
            integration_id: Integration ID
            event_type: Type of event ('integration_connected', 'integration_paused', etc.)
            trigger: What triggered this ('user_action', 'store_action', 'webhook', 'system')
            status: 'success' or 'failed'
            client_id: Client/user who performed the action (optional)
            event_data: Additional event data (dict)
            error_message/code/details: For errors
            ip_address/user_agent: Request metadata for audit trail
            occurred_at: When the event occurred (defaults to now)
        """
        if occurred_at is None:
            occurred_at = datetime.utcnow()
        
        # Validate client_id exists if provided (to avoid FK constraint errors)
        if client_id is not None:
            from models import Client
            client_exists = db_session.query(Client).filter(
                Client.id == client_id,
                Client.is_deleted == False
            ).first()
            if not client_exists:
                logger.warning(f"Client {client_id} not found or deleted, setting client_id to None in audit log")
                client_id = None
        
        log_entry = cls(
            integration_id=integration_id,
            client_id=client_id,
            event_type=event_type,
            trigger=trigger,
            status=status,
            event_data=event_data or {},
            error_message=error_message,
            error_code=error_code,
            error_details=error_details,
            ip_address=ip_address,
            user_agent=user_agent,
            occurred_at=occurred_at,
        )
        
        db_session.add(log_entry)
        db_session.commit()
        db_session.refresh(log_entry)
        
        return log_entry

    @classmethod
    def get_recent_audit_logs(cls, db_session, integration_id: int, limit: int = 100):
        """Get recent audit logs for an integration"""
        return (
            db_session.query(cls)
            .filter(cls.integration_id == integration_id)
            .order_by(cls.occurred_at.desc())
            .limit(limit)
            .all()
        )

    @classmethod
    def get_audit_logs_by_type(cls, db_session, integration_id: int, event_type: str, limit: int = 50):
        """Get audit logs filtered by event type"""
        return (
            db_session.query(cls)
            .filter(
                cls.integration_id == integration_id,
                cls.event_type == event_type,
            )
            .order_by(cls.occurred_at.desc())
            .limit(limit)
            .all()
        )

    @classmethod
    def get_audit_logs_by_client(cls, db_session, client_id: int, limit: int = 100):
        """Get audit logs for a specific client"""
        return (
            db_session.query(cls)
            .filter(cls.client_id == client_id)
            .order_by(cls.occurred_at.desc())
            .limit(limit)
            .all()
        )



