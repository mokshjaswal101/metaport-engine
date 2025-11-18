from pydantic import BaseModel, Json
from sqlalchemy.dialects.postgresql import JSONB
from typing import Optional, Dict, Union
from uuid import UUID
from typing import List

# schema
from schema.base import DBBaseModel


class PickupLocationInsertModel(BaseModel):
    location_name: str
    contact_person_name: str
    contact_person_phone: int
    contact_person_email: str
    alternate_phone: Optional[str] = ""
    address: str
    landmark: Optional[str] = ""
    pincode: int
    city: str
    state: str
    country: str
    location_type: str


class PickupLocationModel(PickupLocationInsertModel, DBBaseModel):
    client_id: int
    company_id: int
    location_code: str
    courier_location_codes: Optional[Dict[str, Union[str, int]]] = {}
    active: bool
    is_default: bool


class PickupLocationResponseModel(PickupLocationInsertModel):
    location_code: str
    active: bool
    is_default: bool
