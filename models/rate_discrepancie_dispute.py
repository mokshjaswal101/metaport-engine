import uuid
import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, Numeric
from sqlalchemy.dialects.postgresql import JSON, INTERVAL, UUID
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import SQLAlchemyError
from database import DBBaseClass, DBBase
from context_manager.context import get_db_session
from logger import logger

# models
# from models import Admin_User_Role, Admin_User, Client


class Admin_Rate_Discrepancie_Dispute(DBBase, DBBaseClass):
    __tablename__ = "rate_discrepancie_dispute"

    awb_number = Column(
        String(255), nullable=True, index=True
    )  # Just for reference, NOT FK
    # discrepancie_id = Column(Integer, ForeignKey("rate_discrepancie.id"), nullable=True)
    product_category = Column(String, nullable=False)
    product_url = Column(String, nullable=False)
    product_remarks = Column(String, nullable=False)
    length_image = Column(String, nullable=True)
    width_image = Column(String, nullable=True)
    height_image = Column(String, nullable=True)
    scale_image = Column(String, nullable=True)
    label_image = Column(String, nullable=True)
