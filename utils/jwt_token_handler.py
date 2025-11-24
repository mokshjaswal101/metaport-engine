from datetime import timedelta, datetime
from pydantic import BaseModel, EmailStr
import jwt
import http
import os
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


# JWT configuration
class JWTToken:
    algorithm = os.getenv("JWT_ALGORITHM", "HS256")
    secret = os.getenv("JWT_SECRET", "secret_key")
    access_token_expire_minutes = os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "300")


class JWTHandler:
    @staticmethod
    def create_access_token(
        to_encode: dict, expires_delta: timedelta = timedelta(minutes=100)
    ):
        # Ensure status is included in token (default to 'active' if not present)
        user_data = to_encode.copy()
        if "status" not in user_data:
            user_data["status"] = "active"

        expire = datetime.now() + timedelta(hours=6)
        user_data.update({"exp": expire.timestamp()})
        encoded_jwt = jwt.encode(
            user_data, JWTToken.secret, algorithm=JWTToken.algorithm
        )

        return encoded_jwt

    @staticmethod
    def create_temp_access_token(to_encode: dict, expires_minutes: int = 15):
        """
        Create a temporary short-lived token for unverified users.
        Used during OTP verification flow.
        """
        # Ensure status is included in token
        user_data = to_encode.copy()
        if "status" not in user_data:
            user_data["status"] = "active"

        expire = datetime.now() + timedelta(minutes=expires_minutes)
        user_data.update({"exp": expire.timestamp(), "temp": True})
        encoded_jwt = jwt.encode(
            user_data, JWTToken.secret, algorithm=JWTToken.algorithm
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

    @staticmethod
    def decode_token_without_context(token: str):
        """
        Decode JWT token and return payload without setting context.
        Used for OTP verification endpoints where user is not yet fully authenticated.
        """
        if not token:
            logger.error(msg="Token is empty or None")
            raise HTTPException(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                detail={"message": "Token is required", "status": False},
            )

        try:
            payload = jwt.decode(
                token, JWTToken.secret, algorithms=[JWTToken.algorithm]
            )

            if not payload:
                logger.error(msg="Decoded payload is None")
                raise HTTPException(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    detail={"message": "Invalid token payload", "status": False},
                )

            logger.info(f"Successfully decoded token, payload keys: {payload.keys()}")
            return payload

        except jwt.ExpiredSignatureError:
            logger.error(msg="Token has expired")
            raise HTTPException(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                detail={"message": "Token has expired", "status": False},
            )
        except jwt.InvalidTokenError as e:
            logger.error(msg=f"Invalid token error: {e}")
            raise HTTPException(
                status_code=http.HTTPStatus.UNAUTHORIZED,
                detail={"message": f"Invalid token: {str(e)}", "status": False},
            )
        except Exception as e:
            logger.error(
                msg=f"Unexpected error while decoding token: {e}",
            )
            raise HTTPException(
                status_code=401,
                detail={
                    "message": "Invalid authentication credentials",
                    "status": False,
                },
            )
