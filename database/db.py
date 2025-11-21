import os
from datetime import datetime
from urllib.parse import quote_plus
import uuid
from pytz import timezone
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine

load_dotenv()

from sqlalchemy import Column, TIMESTAMP, Boolean, Integer, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
)
from sqlalchemy.orm import sessionmaker

from logger import logging


# ----------------------------------------
# 1. DATABASE CONFIG
# ----------------------------------------
DBTYPE_POSTGRES = "postgresql+asyncpg"  # async driver

CORE_SQLALCHEMY_DATABASE_URI = "%s://%s:%s@%s:%s/%s" % (
    DBTYPE_POSTGRES,
    os.environ.get("db_user"),
    quote_plus(os.environ.get("db_password")),
    os.environ.get("db_host"),
    os.environ.get("db_port"),
    os.environ.get("db_name"),
)


# ----------------------------------------
# 2. ASYNC ENGINE (runtime API)
# ----------------------------------------
async_engine = create_async_engine(
    CORE_SQLALCHEMY_DATABASE_URI,
    pool_size=15,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=3600,
    echo=False,
)

# For backward compatibility (old imports using db_engine)
db_engine = async_engine


# ----------------------------------------
# 3. SYNC ENGINE (only for create_all)
# ----------------------------------------
sync_database_uri = CORE_SQLALCHEMY_DATABASE_URI.replace("+asyncpg", "")
sync_engine = create_engine(sync_database_uri, echo=False)


# ----------------------------------------
# 4. Session Makers
# ----------------------------------------
AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    expire_on_commit=False,
    autoflush=False,
    class_=AsyncSession,
)

UTC = timezone("UTC")


def time_now():
    return datetime.now(UTC)


DBBase = declarative_base()


# ----------------------------------------
# 5. Async DB Dependency (FastAPI)
# ----------------------------------------
async def get_db():
    from context_manager.context import context_set_db_session_rollback

    async with AsyncSessionLocal() as session:
        try:
            logging.info("Async DB session created")
            yield session

            if context_set_db_session_rollback.get():
                logging.info("Rolling back async session")
                await session.rollback()
            else:
                logging.info("Committing async session")
                await session.commit()

        except Exception as e:
            logging.error(f"Async DB session error: {e}")
            await session.rollback()
            raise

        finally:
            logging.info("Closing async DB session")
            await session.close()


# ----------------------------------------
# 6. Base Model Class
# ----------------------------------------
class DBBaseClass:
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), default=time_now, nullable=False)
    updated_at = Column(
        TIMESTAMP(timezone=True), default=time_now, onupdate=time_now, nullable=False
    )
    is_deleted = Column(Boolean, default=False)

    @classmethod
    async def get_by_uuid(cls, uuid):
        from context_manager.context import get_async_db_session

        session: AsyncSession = get_async_db_session()
        result = await session.execute(
            cls.__table__.select().where(cls.uuid == uuid, cls.is_deleted.is_(False))
        )
        return result.scalars().first()

    @classmethod
    async def get_by_id(cls, id):
        from context_manager.context import get_async_db_session

        session: AsyncSession = get_async_db_session()
        result = await session.execute(
            cls.__table__.select().where(cls.id == id, cls.is_deleted.is_(False))
        )
        return result.scalars().first()


# ----------------------------------------
# 7. Initialization Function (Sync create_all)
# ----------------------------------------
async def init_models():
    """
    Creates all tables using async engine.
    """
    logging.info("Creating DB tables using async engine...")

    if not isinstance(db_engine, AsyncEngine):
        raise Exception("db_engine is not an AsyncEngine")

    # Run metadata.create_all using async engine
    async with db_engine.begin() as conn:
        await conn.run_sync(DBBase.metadata.create_all)

    logging.info("Tables created successfully.")
