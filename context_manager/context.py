from contextvars import ContextVar
from fastapi import Depends, Request
from sqlalchemy.orm import Session
from logger import logger
from typing import Optional

# models
from database.db import get_db

# defining the context variables to store different types of required data

context_db_session: ContextVar[Session] = ContextVar("db_session", default=None)
context_user_data: ContextVar[str] = ContextVar("user_data", default="")
context_set_db_session_rollback: ContextVar[bool] = ContextVar(
    "set_db_session_rollback", default=False
)


# whenever an api is hit, define the context variables for it
async def build_request_context(db: Session = Depends(get_db)):
    context_db_session.set(db)
    logger.info(msg="REQUEST_INITIATED")


# get the same session everywhere
# the db session is stored in context at the time of the building request context
def get_db_session() -> Session:
    session = context_db_session.get()

    return session


# Helper function to safely get user data from context
def get_user_data():
    """
    Safely get user data from context.
    Returns None if context is not set or user data is invalid.
    """
    user_data = context_user_data.get()
    # Check if user_data is None, empty string, or doesn't have required attributes
    if not user_data or user_data == "" or not hasattr(user_data, 'client_id'):
        return None
    return user_data