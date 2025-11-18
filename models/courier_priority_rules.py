from sqlalchemy import Column, String, Date, JSON, ForeignKey, Integer, Boolean
import http
from sqlalchemy.orm import Session
from database import DBBaseClass, DBBase
from logger import logger
from sqlalchemy.orm import relationship


class Courier_Priority_Rules(DBBase, DBBaseClass):
    __tablename__ = "courier_priority_rules"

    client_id = Column(Integer, nullable=False, index=True)
    ordering_key = Column(Integer, nullable=False, index=True)
    rule_name = Column(String(255), nullable=False, index=True)
    rules = Column(JSON, nullable=False)
    courier_priority = Column(JSON, nullable=False)
    status = Column(Boolean, nullable=False, default=False, index=True)
