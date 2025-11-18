from sqlalchemy import Column, String, Date, JSON, ForeignKey, Integer
import http
from sqlalchemy.orm import Session
from database import DBBaseClass, DBBase
from logger import logger
from sqlalchemy.orm import relationship


class Courier_Priority_Meta(DBBase, DBBaseClass):

    __tablename__ = "courier_priority_meta_options"

    company_id = Column(Integer, nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    courier_type_id = Column(
        Integer,
        ForeignKey("courier_priority.id", ondelete="CASCADE"),
        nullable=False,
    )
    ordering_key = Column(Integer, nullable=False, index=True)
    meta_slug = Column(String(255), nullable=False, index=True)
    meta_value = Column(String(255), nullable=False, index=True)

    courier_priority = relationship("Courier_Priority", back_populates="meta_options")
