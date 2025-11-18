import os
from datetime import datetime
from urllib.parse import quote_plus
import uuid as uuid
from pytz import timezone
from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import Column, TIMESTAMP, Boolean, Integer, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from logger import logging


# logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

# context

# create the DB connection object
DBTYPE_POSTGRES = "postgresql"
CORE_SQLALCHEMY_DATABASE_URI = "%s://%s:%s@%s:%s/%s" % (
    DBTYPE_POSTGRES,
    os.environ.get("db_user"),
    quote_plus(
        os.environ.get("db_password"),
    ),
    os.environ.get("db_host"),
    os.environ.get("db_port"),
    os.environ.get("db_name"),
)

db_engine = create_engine(
    CORE_SQLALCHEMY_DATABASE_URI,
    pool_size=15,  # Reduced from 20
    max_overflow=5,  # Reduced from 10
    pool_timeout=30,  # Increased timeout
    pool_pre_ping=True,
    pool_recycle=3600,  # Increased recycle time to 1 hour
    echo=False,  # Disable SQL logging for performance
)
SessionLocal = sessionmaker(
    # autocommit=False,
    autoflush=False,
    bind=db_engine,
    expire_on_commit=False,  # Prevent attribute expiration on commit
)
UTC = timezone("UTC")


def time_now():
    return datetime.now(UTC)


DBBase = declarative_base()


# this function is used to inject db_session dependency in all the other api requests
def get_db():
    from context_manager.context import context_set_db_session_rollback

    db: Session = SessionLocal()
    try:
        logging.info("DB session created")
        yield db
        #  commit the db session if no exception occurs
        #  if context_set_db_session_rollback is set to True then rollback the db session
        if context_set_db_session_rollback.get():
            logging.info("Rolling back DB session")
            db.rollback()
        else:
            logging.info("Committing DB session")
            db.commit()
    except Exception as e:
        #  rollback the db session if any exception occurs
        logging.error(f"DB session error: {e}")
        db.rollback()
        raise
    finally:
        logging.info("Closing DB session")
        try:
            db.close()
        except Exception as e:
            logging.error(f"Error closing DB session: {e}")


# Base class for all db orm models
class DBBaseClass:

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), default=time_now, nullable=False)
    updated_at = Column(
        TIMESTAMP(timezone=True), default=time_now, onupdate=time_now, nullable=False
    )
    is_deleted = Column(Boolean, default=False)

    @classmethod
    def get_by_uuid(cls, uuid):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        return db.query(cls).filter(cls.uuid == uuid, cls.is_deleted.is_(False)).first()

    @classmethod
    def get_by_id(cls, id):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        return db.query(cls).filter(cls.id == id, cls.is_deleted.is_(False)).first()
