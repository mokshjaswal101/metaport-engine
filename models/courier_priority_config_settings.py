from sqlalchemy import Column, String, Date, JSON, ForeignKey, Integer, Boolean
import http
from sqlalchemy.orm import Session
from database import DBBaseClass, DBBase
from logger import logger
from sqlalchemy.orm import relationship


class Courier_Priority_Config_Setting(DBBase, DBBaseClass):
    __tablename__ = "courier_priority_config_settings"

    company_id = Column(Integer, nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    courier_method = Column(String(255), nullable=False, index=True)
    status = Column(Boolean, nullable=False, index=True)
    # courier_priority = relationship("Courier_Priority", back_populates="meta_options")
