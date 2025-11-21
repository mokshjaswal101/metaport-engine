import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, select, func
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
    alternate_phone = Column(String(15), nullable=True)
    address = Column(String(255), nullable=False)
    landmark = Column(String(255), nullable=True)
    pincode = Column(String(100), nullable=False)
    city = Column(String(100), nullable=False)
    state = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    is_default = Column(Boolean, nullable=False, default=False)
    location_type = Column(String(255), nullable=False, default="warehouse")

    location_code = Column(String(255), unique=True, nullable=False)
    courier_location_codes = Column(JSON, nullable=True)

    company_id = Column(Integer, ForeignKey("company.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    def to_model(self):
        from modules.pickup_location.pickup_location_schema import PickupLocationModel

        return PickupLocationModel.model_validate(self)

    @staticmethod
    async def generate_location_code(db):
        stmt = select(func.count(Pickup_Location.id))
        result = await db.execute(stmt)
        count = result.scalar() or 0

        next_code_number = count + 1
        return f"{next_code_number:04d}"

    @staticmethod
    def create_db_entity(locationRequest):
        return Pickup_Location(**locationRequest)

    @classmethod
    async def create_new_location(cls, db, location):
        db.add(location)
        await db.flush()
        return location.to_model()
