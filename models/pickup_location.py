import json
from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    Boolean,
    Index,
    Sequence,
    text,
)
from sqlalchemy.dialects.postgresql import JSON

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class Pickup_Location(DBBase, DBBaseClass):

    __tablename__ = "pickup_location"

    # Database sequence for generating unique location codes (thread-safe)
    # Defined as class attribute so it's bound to the table metadata
    _location_code_seq = Sequence(
        "pickup_location_code_seq",
        start=1,
        increment=1,
        metadata=DBBase.metadata,  # Bind to the metadata so it gets created with the table
    )

    location_name = Column(String(100), nullable=False)
    contact_person_name = Column(String(100), nullable=False)
    contact_person_phone = Column(String(15), nullable=False)
    contact_person_email = Column(String(255), nullable=False)
    alternate_phone = Column(String(15), nullable=True)  # optional
    address = Column(String(255), nullable=False)
    landmark = Column(String(255), nullable=True)  # optional
    pincode = Column(
        String(100), nullable=False, index=True
    )  # Index for filtering by pincode
    city = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    active = Column(
        Boolean, nullable=False, default=True, index=True
    )  # Index for filtering active locations
    is_default = Column(Boolean, nullable=False, default=False)
    location_type = Column(String(255), nullable=False, default="warehouse")

    location_code = Column(
        String(255), unique=True, nullable=False, index=True
    )  # Index for quick lookups
    courier_location_codes = Column(
        JSON,
        nullable=True,
    )  # optional
    company_id = Column(
        Integer,
        ForeignKey("company.id"),
        nullable=False,
        index=True,  # Index for company filtering
    )
    client_id = Column(
        Integer, ForeignKey("client.id"), nullable=False, index=True
    )  # Index for client filtering

    # Composite indexes for common query patterns
    __table_args__ = (
        # Index for fetching all locations for a client (most common query)
        Index(
            "ix_pickup_location_client_company_deleted",
            "client_id",
            "company_id",
            "is_deleted",
        ),
        # Index for finding default location quickly
        Index("ix_pickup_location_client_default", "client_id", "is_default"),
        # Index for filtering active locations by client
        Index("ix_pickup_location_client_active", "client_id", "active", "is_deleted"),
    )

    def to_model(self):
        from modules.pickup_location.pickup_location_schema import PickupLocationModel

        return PickupLocationModel.model_validate(self)

    def create_db_entity(locationRequest):
        return Pickup_Location(**locationRequest)

    @staticmethod
    def generate_location_code(db=None) -> str:
        """
        Generate unique location code using database sequence.
        Thread-safe and handles concurrent requests properly.
        """
        if db is None:
            db = get_db_session()

        # Use database sequence for thread-safe unique code generation
        next_val = db.execute(
            text("SELECT nextval('pickup_location_code_seq')")
        ).scalar()

        # Format the code to ensure at least 4 digits
        location_code = f"{next_val:04d}"
        return location_code

    @classmethod
    def create_new_location(cls, location):
        db = get_db_session()

        db.add(location)
        db.flush()
        return location.to_model()
