from sqlalchemy import Column, String, Boolean, Integer
from sqlalchemy.dialects.postgresql import UUID
import uuid
from database import DBBaseClass, DBBase


class Qc(DBBase, DBBaseClass):
    __tablename__ = "qc_items"

    client_id = Column(Integer, nullable=False)
    category = Column(String, nullable=False)
    reason_name = Column(String, nullable=False)
    brand_name = Column(String, nullable=True)
    item_name = Column(String, nullable=False)
    item_description = Column(String, nullable=False)
    is_mandatory = Column(Boolean, default=False)
