from pydantic import BaseModel
from typing import Optional, Dict, Union, List

# schema
from schema.base import DBBaseModel


class PickupLocationInsertModel(BaseModel):
    """Model for creating a new pickup location"""

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
    is_default: Optional[bool] = False  # Allow setting default when creating


class PickupLocationUpdateModel(BaseModel):
    """Model for updating a pickup location - all fields optional"""

    location_name: Optional[str] = None
    contact_person_name: Optional[str] = None
    contact_person_phone: Optional[int] = None
    contact_person_email: Optional[str] = None
    alternate_phone: Optional[str] = None
    address: Optional[str] = None
    landmark: Optional[str] = None
    pincode: Optional[int] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    location_type: Optional[str] = None


class PickupLocationModel(PickupLocationInsertModel, DBBaseModel):
    """Full pickup location model with all fields"""

    client_id: int
    company_id: int
    location_code: str
    courier_location_codes: Optional[Dict[str, Union[str, int]]] = {}
    active: bool
    is_default: bool


class PickupLocationResponseModel(BaseModel):
    """Response model for pickup locations with orders count"""

    location_code: str
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
    active: bool
    is_default: bool
    orders_count: int = 0  # Total orders from this location


class PaginationInfo(BaseModel):
    """Pagination metadata"""

    page: int
    page_size: int
    total_count: int
    total_pages: int
    has_next: bool
    has_prev: bool


class PaginatedPickupLocationResponse(BaseModel):
    """Paginated response for pickup locations"""

    locations: List[PickupLocationResponseModel]
    pagination: PaginationInfo
