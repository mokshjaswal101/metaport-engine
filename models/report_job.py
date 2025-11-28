"""
ReportJob Model

Tracks background report generation jobs. Each record represents a single
report generation request with its status, file location, and metadata.

Status Flow:
    PENDING → PROCESSING → COMPLETED
        │          │
        │          └→ FAILED → (retry) → PROCESSING
        │
        └→ CANCELLED
"""

import os
from enum import Enum
from datetime import datetime, timedelta
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
from database import DBBaseClass, DBBase


class ReportJobStatus(str, Enum):
    """Status states for report generation jobs."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReportType(str, Enum):
    """
    Available report types.
    Add new report types here as they are implemented.
    """

    PICKUP_LOCATIONS = "pickup_locations"
    # Future report types:
    # ORDERS_MIS = "orders_mis"
    # SHIPMENT_REPORT = "shipment_report"
    # NDR_REPORT = "ndr_report"
    # RTO_ANALYSIS = "rto_analysis"
    # COD_REMITTANCE = "cod_remittance"
    # COURIER_PERFORMANCE = "courier_performance"
    # BILLING_SUMMARY = "billing_summary"
    # WEIGHT_DISCREPANCY = "weight_discrepancy"


class ReportFormat(str, Enum):
    """Supported report output formats."""

    CSV = "csv"
    # Future formats:
    # XLSX = "xlsx"
    # PDF = "pdf"


# Default retention period from environment (fallback to 30 days)
DEFAULT_RETENTION_DAYS = int(os.environ.get("REPORT_RETENTION_DAYS", "30"))


class ReportJob(DBBase, DBBaseClass):
    """
    Model for tracking report generation jobs.

    Stores all metadata about report requests, including status,
    filters, file location, and timing information.
    """

    __tablename__ = "report_jobs"

    # Ownership - who requested the report
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False, index=True)
    company_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=True)
    user_email = Column(String(255), nullable=True)

    # Report Configuration
    report_type = Column(
        String(50), nullable=False, index=True
    )  # Value from ReportType enum
    report_name = Column(String(255), nullable=False)  # Human-readable name
    report_format = Column(String(10), default=ReportFormat.CSV.value)
    filters = Column(JSON, nullable=True)  # Date range, status filters, etc.

    # Status Tracking
    status = Column(
        String(20), default=ReportJobStatus.PENDING.value, nullable=False, index=True
    )
    progress_percentage = Column(Integer, default=0)
    records_count = Column(Integer, nullable=True)

    # File Storage
    file_url = Column(String(500), nullable=True)  # S3 URL
    file_name = Column(String(255), nullable=True)
    file_size_bytes = Column(Integer, nullable=True)
    s3_key = Column(String(500), nullable=True)  # S3 key for deletion

    # Timing
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    expires_at = Column(DateTime, nullable=True)  # Auto-delete from S3 after this

    # Error Handling & Retries
    error_message = Column(Text, nullable=True)
    error_code = Column(String(50), nullable=True)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)

    # Celery Task Reference
    celery_task_id = Column(String(255), nullable=True)

    # Indexes for common queries
    __table_args__ = (
        Index("ix_report_jobs_client_status", "client_id", "status"),
        Index("ix_report_jobs_client_created", "client_id", "created_at"),
        Index("ix_report_jobs_expires_at", "expires_at"),
    )

    # Relationships
    client = relationship("Client", lazy="noload")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Set default expiry if not provided
        if not kwargs.get("expires_at"):
            self.expires_at = datetime.utcnow() + timedelta(days=DEFAULT_RETENTION_DAYS)

    def to_dict(self) -> dict:
        """Convert model to dictionary for API responses."""
        return {
            "id": self.id,
            "uuid": str(self.uuid) if self.uuid else None,
            "client_id": self.client_id,
            "report_type": self.report_type,
            "report_name": self.report_name,
            "report_format": self.report_format,
            "filters": self.filters,
            "status": self.status,
            "progress_percentage": self.progress_percentage,
            "records_count": self.records_count,
            "file_name": self.file_name,
            "file_size_bytes": self.file_size_bytes,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "duration_seconds": self.duration_seconds,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def to_list_dict(self) -> dict:
        """Minimal dictionary for list views (downloads table)."""
        return {
            "id": self.id,
            "report_type": self.report_type,
            "report_name": self.report_name,
            "report_format": self.report_format,
            "status": self.status,
            "records_count": self.records_count,
            "file_size_bytes": self.file_size_bytes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
            "error_message": (
                self.error_message
                if self.status == ReportJobStatus.FAILED.value
                else None
            ),
        }

    def mark_processing(self):
        """Mark job as processing (called when worker picks up the task)."""
        self.status = ReportJobStatus.PROCESSING.value
        self.started_at = datetime.utcnow()

    def mark_completed(
        self,
        file_url: str,
        file_name: str,
        s3_key: str,
        records_count: int,
        file_size_bytes: int = None,
    ):
        """Mark job as completed with file information."""
        self.status = ReportJobStatus.COMPLETED.value
        self.completed_at = datetime.utcnow()
        self.file_url = file_url
        self.file_name = file_name
        self.s3_key = s3_key
        self.records_count = records_count
        self.file_size_bytes = file_size_bytes
        self.progress_percentage = 100

        if self.started_at:
            self.duration_seconds = int(
                (self.completed_at - self.started_at).total_seconds()
            )

    def mark_failed(self, error_message: str, error_code: str = None):
        """Mark job as failed with error information."""
        self.status = ReportJobStatus.FAILED.value
        self.completed_at = datetime.utcnow()
        self.error_message = error_message
        self.error_code = error_code

        if self.started_at:
            self.duration_seconds = int(
                (self.completed_at - self.started_at).total_seconds()
            )

    def mark_cancelled(self):
        """Mark job as cancelled."""
        self.status = ReportJobStatus.CANCELLED.value
        self.completed_at = datetime.utcnow()

    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return (
            self.status == ReportJobStatus.FAILED.value
            and self.retry_count < self.max_retries
        )

    def increment_retry(self):
        """Increment retry count and reset for new attempt."""
        self.retry_count += 1
        self.status = ReportJobStatus.PENDING.value
        self.error_message = None
        self.error_code = None
        self.started_at = None
        self.completed_at = None
        self.duration_seconds = None
        self.progress_percentage = 0

    @classmethod
    def get_by_id_and_client(cls, db_session, job_id: int, client_id: int):
        """Get a report job by ID, ensuring it belongs to the client."""
        return (
            db_session.query(cls)
            .filter(
                cls.id == job_id,
                cls.client_id == client_id,
                cls.is_deleted == False,
            )
            .first()
        )

    @classmethod
    def get_downloads_for_client(
        cls,
        db_session,
        client_id: int,
        page: int = 1,
        page_size: int = 10,
        status: str = None,
    ):
        """
        Get paginated report jobs for a client.

        Args:
            db_session: Database session
            client_id: Client ID to filter by
            page: Page number (1-indexed)
            page_size: Items per page
            status: Optional status filter

        Returns:
            Tuple of (jobs list, total count)
        """
        query = db_session.query(cls).filter(
            cls.client_id == client_id,
            cls.is_deleted == False,
        )

        if status:
            query = query.filter(cls.status == status)

        # Get total count
        total_count = query.count()

        # Get paginated results
        jobs = (
            query.order_by(cls.created_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )

        return jobs, total_count

    @classmethod
    def get_expired_jobs(cls, db_session, batch_size: int = 100):
        """Get jobs that have expired and need file cleanup."""
        return (
            db_session.query(cls)
            .filter(
                cls.expires_at <= datetime.utcnow(),
                cls.status == ReportJobStatus.COMPLETED.value,
                cls.s3_key.isnot(None),
                cls.is_deleted == False,
            )
            .limit(batch_size)
            .all()
        )

    @classmethod
    def get_pending_jobs(cls, db_session, limit: int = 10):
        """Get pending jobs for processing (used by worker)."""
        return (
            db_session.query(cls)
            .filter(
                cls.status == ReportJobStatus.PENDING.value,
                cls.is_deleted == False,
            )
            .order_by(cls.created_at.asc())
            .limit(limit)
            .all()
        )
