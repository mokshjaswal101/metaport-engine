from datetime import timedelta, datetime
from pydantic import BaseModel, EmailStr
import jwt
import http
from fastapi import HTTPException

from context_manager.context import context_user_data
from logger import logger


# schema
class UserDataModel(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr
    status: str
    client_id: int
    company_id: int
    id: int


# utils
from .environment import Environment


# JWT configuration
class JWTToken:
    algorithm = Environment.get_string("JWT_ALGORITHM", "HS256")
    secret = Environment.get_string("JWT_SECRET", "secret_key")
    access_token_expire_minutes = Environment.get_string(
        "JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "300"
    )


class JWTHandler:
    @staticmethod
    def create_access_token(
        to_encode: dict, expires_delta: timedelta = timedelta(minutes=100)
    ):

        expire = datetime.now() + timedelta(hours=6)
        to_encode.update({"exp": expire.timestamp()})
        encoded_jwt = jwt.encode(
            to_encode, JWTToken.secret, algorithm=JWTToken.algorithm
        )

        return encoded_jwt

    @staticmethod
    def decode_access_token(token: str):
        try:
            payload = jwt.decode(
                token, JWTToken.secret, algorithms=[JWTToken.algorithm]
            )

            context_user_data.set(UserDataModel(**payload))

        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                detail={"message": "Token has expired", "status": False},
            )
        except Exception as e:
            logger.error(
                extra=context_user_data.get(),
                msg=f"Error while decoding access token: {e}",
            )
            raise HTTPException(
                status_code=401,
                detail={
                    "message": "Invalid authentication credentials",
                    "status": False,
                },
            )
