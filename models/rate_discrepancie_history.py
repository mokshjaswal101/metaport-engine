import uuid
import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, Numeric
from sqlalchemy.dialects.postgresql import JSON, INTERVAL, UUID
from sqlalchemy.orm import relationship, Session
from decimal import Decimal
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import TypeDecorator, Numeric
from database import DBBaseClass, DBBase
from context_manager.context import get_db_session
from logger import logger

# models
from models import Client


class DecimalToFloat(TypeDecorator):
    """Convert SQLAlchemy Numeric (Decimal) fields to float automatically."""

    impl = Numeric

    def process_bind_param(self, value, dialect):
        if isinstance(value, Decimal):
            return float(value)
        return value

    def process_result_value(self, value, dialect):
        if isinstance(value, Decimal):
            return float(value)
        return value


class Admin_Rate_Discrepancie_History(DBBase, DBBaseClass):
    __tablename__ = "rate_discrepancie_history"

    awb_number = Column(
        String(255), nullable=True, index=True
    )  # Just for reference, NOT FK
    discrepancie_id = Column(
        Integer, ForeignKey("rate_discrepancie.id"), nullable=True, index=True
    )
    length = Column(DecimalToFloat(10, 3), nullable=False)  # Now stores as float
    height = Column(DecimalToFloat(10, 3), nullable=False)
    width = Column(DecimalToFloat(10, 3), nullable=False)
    volumetric_weight = Column(DecimalToFloat(10, 3), nullable=False)
    dead_weight = Column(DecimalToFloat(10, 3), nullable=True)
    applied_weight = Column(Numeric(10, 3), nullable=False)
    courier_weight = Column(Numeric(10, 3), nullable=False)
    charged_weight = Column(Numeric(10, 3), nullable=False)
    charged_weight_charge = Column(JSON, nullable=True)
    excess_weight_charge = Column(JSON, nullable=True)
    image1 = Column(String, nullable=True)
    image2 = Column(String, nullable=True)
    image3 = Column(String, nullable=True)
    action_by = Column(String, nullable=True, index=True)
    status = Column(String(255), nullable=False, index=True)

    # Relationship with Admin_Rate_Discrepancie
    discrepancie = relationship("Admin_Rate_Discrepancie", back_populates="history")
    # order = relationship("Order", back_populates="rate_discrepancies")

    # def create_db_entity(self):
    #     """Converts the object to an entity for database insertion."""
    #     return Admin_Report_Download(
    #         client_id=self.client_id,
    #         duration=self.duration,
    #         file=self.file,
    #         download_count=self.download_count,
    #         status=self.status,
    #     )
