import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, select, func
from sqlalchemy.dialects.postgresql import JSON

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class Pickup_Location_New(DBBase, DBBaseClass):

    __tablename__ = "pickup_location_new"

    location_name = Column(String(100), nullable=False)
    contact_person_name = Column(String(100), nullable=False)
