from sqlalchemy import Column, String, Integer, JSON, Text, Index, text
from database.db import DBBaseClass, DBBase


class AuditLog(DBBase, DBBaseClass):
    """
    Audit log for security events
    Stores authentication, authorization, and security-related events
    """
    __tablename__ = "audit_logs"

    # Event details
    event_type = Column(String(50), nullable=False)  # login, logout, otp_sent, etc.
    event_category = Column(String(50), nullable=False)  # authentication, authorization, security
    severity = Column(String(20), nullable=False)  # info, warning, error, critical

    # User information
    user_id = Column(Integer, nullable=True)
    user_email = Column(String(255), nullable=True)

    # Event message
    message = Column(Text, nullable=False)

    # Additional context (JSON)
    context = Column(JSON, nullable=True)

    # Request metadata
    ip_address = Column(String(45), nullable=True)  # IPv6 support
    user_agent = Column(Text, nullable=True)
    endpoint = Column(String(255), nullable=True)

    # Indexes for audit log queries and security monitoring
    __table_args__ = (
        # Composite index for user audit history queries
        Index(
            'idx_audit_user_history',
            'user_id',
            text('created_at DESC'),
            postgresql_where=text('is_deleted = false')
        ),
        # Composite index for event type queries
        Index(
            'idx_audit_event_type',
            'event_type',
            text('created_at DESC'),
            postgresql_where=text('is_deleted = false')
        ),
        # Index for security event queries
        Index(
            'idx_audit_security_events',
            'event_category',
            'severity',
            text('created_at DESC'),
            postgresql_where=text("is_deleted = false AND event_category = 'security'")
        ),
        # Index for filtering by IP address (for security monitoring)
        Index(
            'idx_audit_ip_address',
            'ip_address',
            text('created_at DESC'),
            postgresql_where=text('is_deleted = false')
        ),
        # Index for user email lookups in audit logs
        Index(
            'idx_audit_user_email',
            'user_email',
            text('created_at DESC'),
            postgresql_where=text('is_deleted = false')
        ),
    )
    
    def __repr__(self):
        return f"<AuditLog(id={self.id}, event_type={self.event_type}, user_id={self.user_id})>"
    
    @classmethod
    def create_log(cls, log_data: dict):
        """Create a new audit log entry"""
        from context_manager.context import get_db_session
        
        db = get_db_session()
        audit_log = cls(**log_data)
        db.add(audit_log)
        db.flush()
        
        return audit_log
    
    @classmethod
    def get_user_logs(cls, user_id: int, limit: int = 100):
        """Get audit logs for a specific user"""
        from context_manager.context import get_db_session
        
        db = get_db_session()
        return (
            db.query(cls)
            .filter(cls.user_id == user_id, cls.is_deleted.is_(False))
            .order_by(cls.created_at.desc())
            .limit(limit)
            .all()
        )
    
    @classmethod
    def get_logs_by_event_type(cls, event_type: str, limit: int = 100):
        """Get audit logs by event type"""
        from context_manager.context import get_db_session
        
        db = get_db_session()
        return (
            db.query(cls)
            .filter(cls.event_type == event_type, cls.is_deleted.is_(False))
            .order_by(cls.created_at.desc())
            .limit(limit)
            .all()
        )
    
    @classmethod
    def get_security_events(cls, severity: str = None, limit: int = 100):
        """Get security events, optionally filtered by severity"""
        from context_manager.context import get_db_session
        
        db = get_db_session()
        query = db.query(cls).filter(
            cls.event_category == "security",
            cls.is_deleted.is_(False)
        )
        
        if severity:
            query = query.filter(cls.severity == severity)
        
        return query.order_by(cls.created_at.desc()).limit(limit).all()

