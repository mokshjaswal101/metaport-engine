from contextvars import ContextVar
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from database.db import get_db
from logger import logger

# -------------------------------
# Context variables for async
# -------------------------------
context_db_session: ContextVar[AsyncSession] = ContextVar("db_session", default=None)
context_user_data: ContextVar[str] = ContextVar("user_data", default="")
context_set_db_session_rollback: ContextVar[bool] = ContextVar(
    "set_db_session_rollback", default=False
)


# -------------------------------
# Dependency to set async context per request
# -------------------------------
async def build_request_context(db: AsyncSession = Depends(get_db)):
    context_db_session.set(db)
    logger.info(msg="REQUEST_INITIATED")


# -------------------------------
# Retrieve async session anywhere in service
# -------------------------------
def get_db_session() -> AsyncSession:
    return context_db_session.get()
