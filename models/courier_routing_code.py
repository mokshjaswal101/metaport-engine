from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase

import uuid as uuid


class Courier_Routing_Code(DBBase, DBBaseClass):

    __tablename__ = "courier_routing_code"

    pincode = Column(Integer, nullable=False)
    bluedart_routing_code = Column(String(50), nullable=False)
    bluedart_cluster_code = Column(String(50), nullable=False)
