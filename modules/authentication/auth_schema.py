from pydantic import BaseModel, validator, EmailStr

# schema
from modules.user.user_schema import UserModel


class UserLoginModel(BaseModel):
    email: EmailStr
    password: str


class UserDataModel(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    phone: str
    status: str
    client_name: str
    company_name: str
    is_onboarding_completed: bool = False


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
