import uuid
import json
from enum import Enum
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, Numeric
from sqlalchemy.dialects.postgresql import JSON, INTERVAL, UUID
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import SQLAlchemyError

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session
from logger import logger

# models
from models import Client


class Discrepancie_Type(str, Enum):
    forward = "forward"
    rto = "rto"
    both = "both"


class Admin_Rate_Discrepancie(DBBase, DBBaseClass):
    __tablename__ = "rate_discrepancie"
    awb_number = Column(
        String(255), nullable=True, index=True
    )  # Just for reference, NOT FK
    client_id = Column(Integer, nullable=True, index=True)
    order_id = Column(Integer, ForeignKey("order.id"), nullable=True, index=True)
    length = Column(Numeric(10, 3), nullable=False)
    height = Column(Numeric(10, 3), nullable=False)
    width = Column(Numeric(10, 3), nullable=False)

    volumetric_weight = Column(Numeric(10, 3), nullable=False)
    dead_weight = Column(Numeric(10, 3), nullable=True)

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
    discrepancie_type = Column(
        SqlEnum(Discrepancie_Type),
        nullable=False,
        index=True,
    )

    order = relationship("Order", back_populates="rate_discrepancies")

    # Relationship to History (Lazy Loading: select)
    history = relationship(
        "Admin_Rate_Discrepancie_History",
        back_populates="discrepancie",
        # lazy="subquery",
    )

    # Define the relationship using primaryjoin
    disputes = relationship(
        "Admin_Rate_Discrepancie_Dispute",
        primaryjoin="Admin_Rate_Discrepancie.awb_number == foreign(Admin_Rate_Discrepancie_Dispute.awb_number)",
        backref="rate_discrepancy",
        lazy="joined",  # Load disputes when querying discrepancie
    )

    # def create_db_entity(self):
    #     """Converts the object to an entity for database insertion."""
    #     return Admin_Report_Download(
    #         client_id=self.client_id,
    #         duration=self.duration,
    #         file=self.file,
    #         download_count=self.download_count,
    #         status=self.status,
    #     )
