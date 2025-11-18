import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import JSON

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class Pickup_Location(DBBase, DBBaseClass):

    __tablename__ = "pickup_location"

    location_name = Column(String(100), nullable=False)
    contact_person_name = Column(String(100), nullable=False)
    contact_person_phone = Column(String(15), nullable=False)
    contact_person_email = Column(String(255), nullable=False)
    alternate_phone = Column(String(15), nullable=True)  # optional
    address = Column(String(255), nullable=False)
    landmark = Column(String(255), nullable=True)  # optional
    pincode = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    is_default = Column(Boolean, nullable=False, default=False)
    location_type = Column(String(255), nullable=False, default="warehouse")

    location_code = Column(String(255), unique=True, nullable=False)
    courier_location_codes = Column(
        JSON,
        nullable=True,
    )  # optional
    company_id = Column(
        Integer,
        ForeignKey("company.id"),
        nullable=False,
    )
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    def to_model(self):
        from modules.pickup_location.pickup_location_schema import PickupLocationModel

        return PickupLocationModel.model_validate(self)

    def create_db_entity(locationRequest):
        return Pickup_Location(**locationRequest)

    @staticmethod
    def generate_location_code() -> str:
        db = get_db_session()
        count = db.query(Pickup_Location).count()

        # Increment count to get the next code
        next_code_number = count + 1

        # Format the code to ensure at least 4 digits
        location_code = f"{next_code_number:04d}"
        return location_code

    @classmethod
    def create_new_location(cls, location):
        db = get_db_session()

        db.add(location)
        db.flush()
        return location.to_model()
