from pydantic import BaseModel, validator, EmailStr
import re

# schema
from modules.user.user_schema import UserModel


class UserLoginModel(BaseModel):
    email: EmailStr
    password: str

    @validator("email", pre=True)
    def sanitize_email(cls, v):
        """Sanitize email: trim whitespace and convert to lowercase."""
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @validator("password")
    def sanitize_password(cls, v):
        """Sanitize password: remove leading/trailing whitespace."""
        if isinstance(v, str):
            return v.strip()
        return v


class UserDataModel(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    phone: str
    status: str
    client_name: str
    company_name: str
    is_otp_verified: bool = False


class LoginResponseUserData(BaseModel):
    access_token: str
    user_data: UserDataModel


class UserRegisterModel(BaseModel):
    client_name: str
    user_first_name: str
    user_last_name: str
    user_email: str
    user_phone: str
    password: str

    @validator("client_name", pre=True)
    def sanitize_client_name(cls, v):
        """Sanitize client name: trim whitespace and normalize spaces."""
        if isinstance(v, str):
            return re.sub(r'\s+', ' ', v.strip())
        return v

    @validator("user_first_name", pre=True)
    def sanitize_first_name(cls, v):
        """Sanitize first name: trim whitespace and normalize spaces."""
        if isinstance(v, str):
            return re.sub(r'\s+', ' ', v.strip())
        return v

    @validator("user_last_name", pre=True)
    def sanitize_last_name(cls, v):
        """Sanitize last name: trim whitespace and normalize spaces."""
        if isinstance(v, str):
            return re.sub(r'\s+', ' ', v.strip())
        return v

    @validator("user_email", pre=True)
    def sanitize_user_email(cls, v):
        """Sanitize email: trim whitespace and convert to lowercase."""
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @validator("user_phone", pre=True)
    def sanitize_phone(cls, v):
        """Sanitize phone: remove all non-digit characters and validate length."""
        if isinstance(v, str):
            # Remove all non-digit characters
            digits_only = re.sub(r'\D', '', v.strip())
            # Validate it's exactly 10 digits
            if len(digits_only) == 10:
                return digits_only
            return v.strip()
        return v

    @validator("password")
    def sanitize_password(cls, v):
        """Sanitize password: remove leading/trailing whitespace."""
        if isinstance(v, str):
            return v.strip()
        return v
