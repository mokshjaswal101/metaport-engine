import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, FLOAT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class PaymentRecords(DBBase, DBBaseClass):

    __tablename__ = "payment_records"

    gateway = Column(String(100), nullable=False)
    payment_id = Column(String(100), nullable=True)
    order_id = Column(String(100), nullable=False)
    status = Column(String(100), nullable=False)
    amount = Column(FLOAT(10, 3), nullable=True)
    currency = Column(String(100), nullable=False)
    method = Column(String(100), nullable=True)
    type = Column(String(100), nullable=False)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    client = relationship("Client", lazy="noload")

    def to_model(self):
        from modules.razorpay.razorpay_schema import PaymentRecordModel

        return PaymentRecordModel.model_validate(self)

    def create_db_entity(payment):
        return PaymentRecords(**payment)
