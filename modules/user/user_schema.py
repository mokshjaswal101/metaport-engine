from pydantic import BaseModel, validator, EmailStr
from typing import Optional
from enum import Enum

# schema
from schema.base import DBBaseModel


class UserBaseModel(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    phone: str

    extra_credentials: str = ""


class UserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class UserResponseModel(UserBaseModel):
    company_id: int
    client_id: int
    status: UserStatus


class UserInsertModel(UserResponseModel):
    status: UserStatus = UserStatus.ACTIVE
    password: str

    @validator("password")
    def password_validator(cls, password):
        """
        Validates that the password is at least 8 characters long,
        contains at least one uppercase letter, one lowercase letter,
        one number, and one special character.
        """
        special_chars = {
            "!",
            "@",
            "#",
            "$",
            "%",
            "^",
            "&",
            "*",
            "(",
            ")",
            "-",
            "+",
            "=",
        }
        if len(password) < 8:
            raise ValueError("password must be at least 8 characters long")
        # if not any(char.isupper() for char in password):
        #     raise ValueError("password must contain at least one uppercase letter")
        if not any(char.islower() for char in password):
            raise ValueError("password must contain at least one lowercase letter")
        if not any(char.isdigit() for char in password):
            raise ValueError("password must contain at least one number")
        if not any(char in special_chars for char in password):
            raise ValueError("password must contain at least one special character")
        return password


class UserModel(DBBaseModel, UserBaseModel):
    status: UserStatus
    company_id: int
    client_id: int
    password_hash: str
    extra_credentials: Optional[str] = None


class ChangePasswordModel(BaseModel):
    old_password: str
    new_password: str
