from sqlalchemy import Column, String, Date, JSON, Integer, desc
import http
from sqlalchemy.orm import Session
from database import DBBaseClass, DBBase
from logger import logger
from sqlalchemy.orm import relationship
from models import Courier_Priority_Meta


class Courier_Priority(DBBase, DBBaseClass):

    __tablename__ = "courier_priority"

    company_id = Column(Integer, nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    priority_type = Column(String(255), nullable=False, index=True)

    # Relationship with Courier_Priority_Meta
    meta_options = relationship(
        "Courier_Priority_Meta",
        back_populates="courier_priority",
        cascade="all, delete-orphan",
        order_by=Courier_Priority_Meta.ordering_key,
    )
