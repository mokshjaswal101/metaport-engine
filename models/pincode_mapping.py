from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase

import uuid as uuid


class Pincode_Mapping(DBBase, DBBaseClass):

    __tablename__ = "pincode_mapping"

    pincode = Column(Integer, nullable=False)
    city = Column(String(50), nullable=False)
    state = Column(String(50), nullable=False)
