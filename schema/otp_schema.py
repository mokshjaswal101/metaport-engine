"""
OTP Verification Schema
Pydantic models for OTP verification data validation and serialization.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from schema.base import DBBaseModel


class OTPVerificationModel(DBBaseModel):
    """
    OTP Verification model for serialization.

    Attributes:
        user_id: ID of the user this OTP belongs to
        otp_code: 6-digit OTP code
        otp_type: Type of OTP ('phone' or 'email')
        expires_at: Expiration timestamp
        verified_at: Timestamp when OTP was verified (if verified)
        attempts: Number of verification attempts made
        is_used: Whether the OTP has been used/verified
    """
    user_id: int
    otp_code: str = Field(..., min_length=6, max_length=6)
    otp_type: str = Field(default="phone", pattern="^(phone|email)$")
    expires_at: datetime
    verified_at: Optional[datetime] = None
    attempts: int = Field(default=0, ge=0)
    is_used: bool = False

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "uuid": "550e8400-e29b-41d4-a716-446655440000",
                "user_id": 123,
                "otp_code": "123456",
                "otp_type": "phone",
                "expires_at": "2025-11-23T10:30:00Z",
                "verified_at": None,
                "attempts": 0,
                "is_used": False,
                "created_at": "2025-11-23T10:20:00Z",
                "updated_at": "2025-11-23T10:20:00Z",
                "is_deleted": False
            }
        }
