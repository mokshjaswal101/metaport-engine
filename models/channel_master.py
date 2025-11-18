from sqlalchemy import Column, String, Integer, Boolean, Text, JSON, DateTime, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from database import DBBaseClass, DBBase


class ChannelMaster(DBBase, DBBaseClass):
    __tablename__ = "channel_master"

    name = Column(String(255), nullable=False, unique=True)
    slug = Column(
        String(100), nullable=False, unique=True
    )  # e.g., 'shopify', 'woocommerce'
    channel_type = Column(
        String(50), nullable=False
    )  # 'marketplace', 'wms', 'erp', 'pos'
    logo_url = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)

    # JSON schema for required credentials
    credentials_schema = Column(JSON, nullable=False)

    # JSON schema for additional configuration
    config_schema = Column(JSON, nullable=True)

    # Status and metadata
    is_active = Column(Boolean, default=True)
    version = Column(String(20), default="1.0")
    documentation_url = Column(String(500), nullable=True)

    # Relationships
    integrations = relationship(
        "ClientChannelIntegration", back_populates="channel", lazy="noload"
    )
    # webhooks = relationship("ChannelWebhook", back_populates="channel", lazy="noload")

    # Indexes for JSON fields (GIN indexes for better JSON query performance)
    # __table_args__ = (
    #     Index("idx_channel_capabilities_gin", "capabilities", postgresql_using="gin"),
    #     Index(
    #         "idx_channel_override_component_gin",
    #         "override_component",
    #         postgresql_using="gin",
    #     ),
    # )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "slug": self.slug,
            "channel_type": self.channel_type,
            "logo_url": self.logo_url,
            "description": self.description,
            "credentials_schema": self.credentials_schema,
            "config_schema": self.config_schema,
            "capabilities": self.capabilities,
            "override_component": self.override_component,
            "is_active": self.is_active,
            "version": self.version,
            "documentation_url": self.documentation_url,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def get_active_channels(cls, db_session):
        """Get all active channels"""
        return db_session.query(cls).filter(cls.is_active == True).all()

    @classmethod
    def get_by_slug(cls, db_session, slug):
        """Get channel by slug"""
        return db_session.query(cls).filter(cls.slug == slug).first()
