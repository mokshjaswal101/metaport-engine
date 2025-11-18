from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,
    Text,
    JSON,
    DateTime,
    ForeignKey,
)
from sqlalchemy.orm import relationship
from datetime import datetime
from database import DBBaseClass, DBBase


class ClientChannelIntegration(DBBase, DBBaseClass):
    __tablename__ = "channel_client_integrations"

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    channel_id = Column(Integer, ForeignKey("channel_master.id"), nullable=False)

    # Integration details
    integration_name = Column(
        String(255), nullable=False
    )  # Custom name given by client

    # Configuration settings (non-sensitive)
    config = Column(JSON, nullable=True)

    # Non-sensitive metadata
    additional_metadata = Column(
        JSON,
        nullable=True,
        default=lambda: {
            "store_url": None,
            "store_timezone": None,
            "currency": None,
            "language": None,
            "integration_version": None,
            "last_health_check": None,
            "performance_metrics": {},
            "custom_tags": [],
        },
    )

    # Sync settings
    auto_sync_enabled = Column(Boolean, default=True)
    sync_interval_minutes = Column(Integer, default=30)  # How often to sync

    # Order sync settings
    order_statuses_to_fetch = Column(JSON, nullable=True)  # ['paid', 'fulfilled', etc.]
    last_order_sync_at = Column(DateTime, nullable=True)
    last_successful_sync_at = Column(DateTime, nullable=True)

    # Webhook settings
    webhook_enabled = Column(Boolean, default=False)
    webhook_url = Column(String(500), nullable=True)
    webhook_secret = Column(String(255), nullable=True)

    # Status and metadata
    is_active = Column(Boolean, default=True)
    connection_status = Column(
        String(50), default="pending"
    )  # 'connected', 'failed', 'pending'
    last_connection_test_at = Column(DateTime, nullable=True)
    connection_error_message = Column(Text, nullable=True)

    # Sync statistics
    total_orders_synced = Column(Integer, default=0)
    total_sync_errors = Column(Integer, default=0)
    last_sync_error_message = Column(Text, nullable=True)

    # Relationships
    client = relationship("Client", lazy="noload")
    channel = relationship(
        "ChannelMaster", back_populates="integrations", lazy="noload"
    )
    sync_logs = relationship(
        "IntegrationSyncLog", back_populates="integration", lazy="noload"
    )
    # credentials_entries = relationship(
    #     "ChannelCredentials", back_populates="integration", lazy="noload"
    # )
    # webhooks = relationship(
    #     "ChannelWebhook", back_populates="integration", lazy="noload"
    # )
    # external_mappings = relationship(
    #     "ExternalMapping", back_populates="integration", lazy="noload"
    # )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            "id": self.id,
            "client_id": self.client_id,
            "channel_id": self.channel_id,
            "integration_name": self.integration_name,
            "config": self.config,
            "auto_sync_enabled": self.auto_sync_enabled,
            "sync_interval_minutes": self.sync_interval_minutes,
            "order_statuses_to_fetch": self.order_statuses_to_fetch,
            "last_order_sync_at": self.last_order_sync_at,
            "last_successful_sync_at": self.last_successful_sync_at,
            "webhook_enabled": self.webhook_enabled,
            "webhook_url": self.webhook_url,
            "is_active": self.is_active,
            "connection_status": self.connection_status,
            "last_connection_test_at": self.last_connection_test_at,
            "connection_error_message": self.connection_error_message,
            "total_orders_synced": self.total_orders_synced,
            "total_sync_errors": self.total_sync_errors,
            "last_sync_error_message": self.last_sync_error_message,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def get_client_integrations(cls, db_session, client_id):
        """Get all integrations for a client"""
        return (
            db_session.query(cls)
            .filter(cls.client_id == client_id, cls.is_active == True)
            .all()
        )

    @classmethod
    def get_active_integrations_for_sync(cls, db_session):
        """Get all active integrations that should be synced"""
        return (
            db_session.query(cls)
            .filter(
                cls.is_active == True,
                cls.auto_sync_enabled == True,
                cls.connection_status == "connected",
            )
            .all()
        )

    def update_sync_stats(self, orders_count=0, error_message=None):
        """Update sync statistics"""
        if error_message:
            self.total_sync_errors += 1
            self.last_sync_error_message = error_message
        else:
            self.total_orders_synced += orders_count
            self.last_successful_sync_at = datetime.utcnow()

        self.last_order_sync_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def update_connection_status(self, status, error_message=None):
        """Update connection status"""
        self.connection_status = status
        self.last_connection_test_at = datetime.utcnow()
        if error_message:
            self.connection_error_message = error_message
        else:
            self.connection_error_message = None
        self.updated_at = datetime.utcnow()
