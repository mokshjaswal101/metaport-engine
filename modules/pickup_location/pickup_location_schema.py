from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Union, List
import re

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

    @field_validator(
        "location_name",
        "contact_person_name",
        "address",
        "landmark",
        "city",
        "state",
        "country",
        "location_type",
    )
    @classmethod
    def sanitize_text_fields(cls, v: str) -> str:
        """Trim whitespace and remove extra spaces from text fields"""
        if v is None:
            return v
        # Strip leading/trailing whitespace
        v = v.strip()
        # Replace multiple spaces with single space
        v = re.sub(r"\s+", " ", v)
        return v

    @field_validator("contact_person_email")
    @classmethod
    def sanitize_email(cls, v: str) -> str:
        """Convert email to lowercase and trim whitespace"""
        if v is None:
            return v
        # Strip whitespace and convert to lowercase
        v = v.strip().lower()
        # Remove any spaces within the email
        v = re.sub(r"\s+", "", v)
        return v

    @field_validator("alternate_phone")
    @classmethod
    def sanitize_alternate_phone(cls, v: Optional[str]) -> str:
        """Remove whitespace and non-numeric characters from alternate phone"""
        if not v:
            return ""
        # Remove all whitespace and non-numeric characters
        v = re.sub(r"\D", "", v)
        return v


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

    @field_validator(
        "location_name",
        "contact_person_name",
        "address",
        "landmark",
        "city",
        "state",
        "country",
        "location_type",
    )
    @classmethod
    def sanitize_text_fields(cls, v: Optional[str]) -> Optional[str]:
        """Trim whitespace and remove extra spaces from text fields"""
        if v is None:
            return v
        # Strip leading/trailing whitespace
        v = v.strip()
        # Replace multiple spaces with single space
        v = re.sub(r"\s+", " ", v)
        return v

    @field_validator("contact_person_email")
    @classmethod
    def sanitize_email(cls, v: Optional[str]) -> Optional[str]:
        """Convert email to lowercase and trim whitespace"""
        if v is None:
            return v
        # Strip whitespace and convert to lowercase
        v = v.strip().lower()
        # Remove any spaces within the email
        v = re.sub(r"\s+", "", v)
        return v

    @field_validator("alternate_phone")
    @classmethod
    def sanitize_alternate_phone(cls, v: Optional[str]) -> Optional[str]:
        """Remove whitespace and non-numeric characters from alternate phone"""
        if not v:
            return v
        # Remove all whitespace and non-numeric characters
        v = re.sub(r"\D", "", v)
        return v


class PickupLocationModel(PickupLocationInsertModel, DBBaseModel):
    """Full pickup location model with all fields"""

    client_id: int
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
