import json
from sqlalchemy import Column, String, Integer, ForeignKey, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from pytz import timezone

from database import DBBaseClass, DBBase


UTC = timezone("UTC")


def time_now():
    return datetime.now(UTC)


class BulkOrderUploadLogs(DBBase, DBBaseClass):

    __tablename__ = "bulk_order_upload_logs"

    upload_date = Column(
        TIMESTAMP(timezone=True), default=time_now, onupdate=time_now, nullable=False
    )
    order_count = Column(Integer, nullable=False)
    uploaded_order_count = Column(Integer, nullable=False)
    error_order_count = Column(Integer, nullable=False)
    error_file_url = Column(String, nullable=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    client = relationship("Client", lazy="noload")

    def to_model(self):
        from modules.orders.order_schema import BulkOrderUploadLogsModel

        return BulkOrderUploadLogsModel.model_validate(self)

    @staticmethod
    def create_db_entity(payment):
        return BulkOrderUploadLogs(**payment)
