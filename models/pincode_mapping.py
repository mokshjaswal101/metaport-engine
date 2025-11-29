from sqlalchemy import Column, String, Integer, Index
from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase

import uuid as uuid


class Pincode_Mapping(DBBase, DBBaseClass):

    __tablename__ = "pincode_mapping"

    # Unique index on pincode for fast lookups and data integrity
    pincode = Column(Integer, nullable=False, unique=True)
    # City and state are stored in lowercase for consistency and case-insensitive comparisons
    city = Column(String(50), nullable=False)
    state = Column(String(50), nullable=False)

    # Composite covering index for queries that fetch pincode, city, state together
    # This allows index-only scans when querying by pincode (avoids table access)
    # Especially beneficial for bulk queries with .in_() operator
    __table_args__ = (
        Index("ix_pincode_mapping_pincode_city_state", "pincode", "city", "state"),
    )
