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


class IntegrationSyncLog(DBBase, DBBaseClass):
    __tablename__ = "channel_sync_logs"

    integration_id = Column(
        Integer, ForeignKey("channel_client_integrations.id"), nullable=False
    )

    # Job tracking
    job_id = Column(
        String(255), nullable=False, unique=True
    )  # Unique identifier for the sync job
    attempt = Column(Integer, default=1)  # Retry attempt number
    parent_id = Column(
        Integer, ForeignKey("channel_sync_logs.id"), nullable=True
    )  # For retry chains

    # Sync details
    sync_type = Column(
        String(50), nullable=False
    )  # 'order_import', 'status_update', 'inventory_sync'
    sync_trigger = Column(
        String(50), nullable=False
    )  # 'scheduled', 'manual', 'webhook'

    # Sync results
    status = Column(
        String(50), nullable=False
    )  # 'success', 'partial_success', 'failed'
    records_processed = Column(Integer, default=0)
    records_successful = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    # Sync metadata
    sync_data = Column(JSON, nullable=True)  # Additional data about the sync
    error_details = Column(JSON, nullable=True)  # Detailed error information

    # Timing
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # Error information
    error_message = Column(Text, nullable=True)
    error_code = Column(String(50), nullable=True)

    # Relationships
    integration = relationship(
        "ClientChannelIntegration", back_populates="sync_logs", lazy="noload"
    )
    parent = relationship(
        "IntegrationSyncLog", remote_side="IntegrationSyncLog.id", lazy="noload"
    )
    retries = relationship("IntegrationSyncLog", back_populates="parent", lazy="noload")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def to_dict(self):
        return {
            "id": self.id,
            "integration_id": self.integration_id,
            "job_id": self.job_id,
            "attempt": self.attempt,
            "parent_id": self.parent_id,
            "sync_type": self.sync_type,
            "sync_trigger": self.sync_trigger,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_successful": self.records_successful,
            "records_failed": self.records_failed,
            "sync_data": self.sync_data,
            "error_details": self.error_details,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message,
            "error_code": self.error_code,
            "created_at": self.created_at,
        }

    def complete_sync(
        self,
        status,
        records_successful=0,
        records_failed=0,
        error_message=None,
        error_details=None,
    ):
        """Mark sync as completed"""
        self.completed_at = datetime.utcnow()
        self.duration_seconds = int(
            (self.completed_at - self.started_at).total_seconds()
        )
        self.status = status
        self.records_successful = records_successful
        self.records_failed = records_failed
        self.records_processed = records_successful + records_failed

        if error_message:
            self.error_message = error_message
        if error_details:
            self.error_details = error_details

    @classmethod
    def get_recent_logs(cls, db_session, integration_id, limit=50):
        """Get recent sync logs for an integration"""
        return (
            db_session.query(cls)
            .filter(cls.integration_id == integration_id)
            .order_by(cls.created_at.desc())
            .limit(limit)
            .all()
        )

    @classmethod
    def get_failed_syncs(cls, db_session, integration_id, hours=24):
        """Get failed syncs in the last N hours"""
        from datetime import timedelta

        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        return (
            db_session.query(cls)
            .filter(
                cls.integration_id == integration_id,
                cls.status == "failed",
                cls.created_at >= cutoff_time,
            )
            .all()
        )
