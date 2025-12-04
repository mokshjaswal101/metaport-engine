"""
Database Configuration Module

Optimized for high-volume operations (lakhs to crores of orders/month).

Connection Pooling Strategy:
- Direct connection: pool_size=30, max_overflow=20 (50 total)
- For production at scale, use PgBouncer as connection pooler

PgBouncer Configuration (recommended for production):
- Mode: Transaction pooling
- Default pool size: 100
- Max client connections: 500
- Reserve pool: 10

To use PgBouncer:
1. Install: apt-get install pgbouncer
2. Configure /etc/pgbouncer/pgbouncer.ini
3. Point db_host to pgbouncer instead of direct PostgreSQL
"""

import os
from datetime import datetime
from urllib.parse import quote_plus
import uuid as uuid
from pytz import timezone
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import Column, TIMESTAMP, Boolean, Integer, create_engine, event
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from logger import logging


# ============================================
# DATABASE CONNECTION CONFIGURATION
# ============================================

DBTYPE_POSTGRES = "postgresql"

# Build connection URI
CORE_SQLALCHEMY_DATABASE_URI = "%s://%s:%s@%s:%s/%s" % (
    DBTYPE_POSTGRES,
    os.environ.get("db_user"),
    quote_plus(os.environ.get("db_password", "")),
    os.environ.get("db_host"),
    os.environ.get("db_port"),
    os.environ.get("db_name"),
)

# ============================================
# CONNECTION POOL SETTINGS
# ============================================

# Pool configuration optimized for high-volume order operations
POOL_CONFIG = {
    # Base pool size - always maintain this many connections
    "pool_size": 30,
    
    # Additional connections allowed during peak load
    "max_overflow": 20,
    
    # Total possible connections: 30 + 20 = 50
    
    # Timeout waiting for a connection from pool (seconds)
    "pool_timeout": 30,
    
    # Test connection health before using (handles stale connections)
    "pool_pre_ping": True,
    
    # Recycle connections after 30 minutes (prevents stale connections)
    "pool_recycle": 1800,
    
    # Disable SQL echo for performance
    "echo": False,
    
    # Use QueuePool for connection management
    "poolclass": QueuePool,
}

# Create engine with optimized settings
db_engine = create_engine(
    CORE_SQLALCHEMY_DATABASE_URI,
    **POOL_CONFIG,
)

# ============================================
# SESSION CONFIGURATION
# ============================================

SessionLocal = sessionmaker(
    autoflush=False,  # Manual flush for better control
    bind=db_engine,
    expire_on_commit=False,  # Prevent attribute expiration on commit
)

# Timezone configuration
UTC = timezone("UTC")
IST = timezone("Asia/Kolkata")


def time_now():
    """Get current UTC time"""
    return datetime.now(UTC)


def time_now_ist():
    """Get current IST time"""
    return datetime.now(IST)


# ============================================
# CONNECTION POOL MONITORING
# ============================================

@event.listens_for(db_engine, "checkout")
def receive_checkout(dbapi_connection, connection_record, connection_proxy):
    """Log when connection is checked out from pool"""
    logging.debug("Connection checked out from pool")


@event.listens_for(db_engine, "checkin")
def receive_checkin(dbapi_connection, connection_record):
    """Log when connection is returned to pool"""
    logging.debug("Connection returned to pool")


def get_pool_status():
    """
    Get current connection pool status.
    Useful for monitoring and debugging.
    
    Returns:
        dict: Pool status including size, checked out, overflow
    """
    pool = db_engine.pool
    return {
        "pool_size": pool.size(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "checked_in": pool.checkedin(),
    }


# ============================================
# DECLARATIVE BASE
# ============================================

DBBase = declarative_base()


# ============================================
# SESSION MANAGEMENT
# ============================================

def get_db():
    """
    Generator function for database session dependency injection.
    
    Usage in FastAPI:
        @router.get("/")
        def endpoint(db: Session = Depends(get_db)):
            ...
    
    Handles:
    - Session creation
    - Automatic commit on success
    - Rollback on error
    - Session cleanup
    """
    from context_manager.context import context_set_db_session_rollback

    db: Session = SessionLocal()
    try:
        logging.debug("DB session created")
        yield db
        
        # Commit or rollback based on context flag
        if context_set_db_session_rollback.get():
            logging.debug("Rolling back DB session")
            db.rollback()
        else:
            logging.debug("Committing DB session")
            db.commit()
            
    except Exception as e:
        logging.error(f"DB session error: {e}")
        db.rollback()
        raise
    finally:
        logging.debug("Closing DB session")
        try:
            db.close()
        except Exception as e:
            logging.error(f"Error closing DB session: {e}")


# ============================================
# BASE MODEL CLASS
# ============================================

class DBBaseClass:
    """
    Base class for all database models.
    
    Provides:
    - Auto-incrementing primary key (id)
    - UUID for external references
    - Created/updated timestamps
    - Soft delete flag
    - Common query methods
    """

    # Primary key
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    
    # UUID for external API references (don't expose internal IDs)
    uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
    # Timestamps
    created_at = Column(TIMESTAMP(timezone=True), default=time_now, nullable=False)
    updated_at = Column(
        TIMESTAMP(timezone=True), 
        default=time_now, 
        onupdate=time_now, 
        nullable=False
    )
    
    # Soft delete
    is_deleted = Column(Boolean, default=False, index=True)

    @classmethod
    def get_by_uuid(cls, uuid):
        """Get record by UUID"""
        from context_manager.context import get_db_session
        db: Session = get_db_session()
        return db.query(cls).filter(cls.uuid == uuid, cls.is_deleted.is_(False)).first()

    @classmethod
    def get_by_id(cls, id):
        """Get record by ID"""
        from context_manager.context import get_db_session
        db: Session = get_db_session()
        return db.query(cls).filter(cls.id == id, cls.is_deleted.is_(False)).first()

    def soft_delete(self):
        """Mark record as deleted (soft delete)"""
        self.is_deleted = True
        self.updated_at = time_now()

    def to_dict(self):
        """
        Convert model to dictionary.
        Override in subclasses for custom serialization.
        """
        return {
            "id": self.id,
            "uuid": str(self.uuid),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
