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


class Ndr(DBBase, DBBaseClass):

    __tablename__ = "ndr"

    # ndr details
    order_id = Column(Integer, ForeignKey("order.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    awb = Column(String, nullable=False)
    status = Column(String, nullable=False)
    datetime = Column(String, nullable=False)
    attempt = Column(Integer, nullable=True)
    alternate_phone_number = Column(String, nullable=True)
    address = Column(String, nullable=True)

    reason = Column(String, nullable=True)

    # Relationship to the Order table (many-to-one)
    order = relationship("Order", back_populates="ndr")

    def create_db_entity(NdrRequest):
        return Ndr(**NdrRequest)

    def to_model(self):

        from modules.ndr.ndr_schema import Ndr_Response_Model

        return Ndr_Response_Model.model_validate(self)

    @classmethod
    def create_new_order(cls, order):
        try:
            db = get_db_session()
            db.add(order)
            db.flush()
            db.commit()

            return order

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                msg="Unhandled error: {}".format(str(e)),
            )

    @classmethod
    def create_new_ndr(cls, ndr):
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
