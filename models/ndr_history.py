from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    Numeric,
    Boolean,
    TIMESTAMP,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON
from pytz import timezone

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class Ndr_history(DBBase, DBBaseClass):

    __tablename__ = "ndr_history"

    # ndr details
    order_id = Column(Integer, ForeignKey("order.id"), nullable=False)
    ndr_id = Column(Integer, ForeignKey("ndr.id"), nullable=False)
    status = Column(String, nullable=False)
    datetime = Column(String, nullable=False)

    reason = Column(String, nullable=True)

    def create_db_entity(NdrHistoryRequest):
        return Ndr_history(**NdrHistoryRequest)

    @classmethod
    def create_new_ndr_history(cls, ndr):
        try:
            db = get_db_session()
            db.add(ndr)
            db.flush()
            db.commit()

            return ndr

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                msg="Unhandled error: {}".format(str(e)),
            )
