import http
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from context_manager.context import build_request_context
from database.db import get_db

# schema
from schema.base import GenericResponseModel
from .user_schema import (
    UserInsertModel,
    ChangePasswordModel,
    OTPVerifyRequestModel,
    PhoneNumberUpdateModel,
)

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler
from utils.otp_handler import OTPHandler

# service
from .user_service import UserService

# Security for token validation (but not full authentication)
security = HTTPBearer()

# Creating user router (for authenticated endpoints only)
user_router = APIRouter(tags=["user"], prefix="/user")

# Creating OTP router (for unauthenticated OTP verification flow)
# This router should NOT be included in CommonRouter with full auth
# Keeping the same route structure: /api/v1/user/otp/...
otp_router = APIRouter(tags=["otp"], prefix="/api/v1/user/otp")


@user_router.post(
    "/register",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_new_client(
    user_data: UserInsertModel,
):
    response: GenericResponseModel = UserService.create_user(user_data=user_data)
    return build_api_response(response)


@user_router.post(
    "/profile/change-password",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def change_user_password(
    user_data: ChangePasswordModel,
):
    response: GenericResponseModel = UserService.change_password(user_data=user_data)
    return build_api_response(response)


# ==================== OTP ENDPOINTS ====================
# These endpoints are for users who are NOT yet OTP verified
# They should NOT be behind full authentication middleware


@otp_router.post(
    "/verify",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def verify_otp(
    otp_request: OTPVerifyRequestModel,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    _=Depends(build_request_context),
):
    """
    Verify OTP for user during registration/login flow.
    This endpoint does NOT require full authentication - only a valid JWT token.
    The user_id is extracted from the token without checking is_otp_verified status.
    After successful verification, returns a new full JWT token.
    """

    try:
        # Decode token to get user_id (without full authentication check)
        token = credentials.credentials
        payload = JWTHandler.decode_token_without_context(token)

        if not payload:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    status=False,
                    message="Invalid token: payload is empty",
                )
            )

        user_id = payload.get("id")

        if not user_id:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    status=False,
                    message="Invalid token: user_id not found",
                )
            )

        # Verify OTP for this user
        response: GenericResponseModel = OTPHandler.verify_otp(
            user_id=user_id,
            otp_code=otp_request.otp,
            mark_user_verified=True,
            purpose="user_registration",
        )

        # If OTP verification successful, issue a new full JWT token
        if response.status:
            from models.user import User

            user = User.get_by_id(user_id)
            if user:
                # Create full JWT token (6 hours validity)
                user_dict = {
                    "id": user.id,
                    "uuid": str(user.uuid),
                    "email": user.email,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "phone": user.phone,
                    "is_otp_verified": True,  # Now verified
                    "status": user.status,
                    "company_id": user.company_id,
                    "client_id": user.client_id,  # IMPORTANT: Include for authorization
                }

                new_token = JWTHandler.create_access_token(user_dict)

                # Add token and user data to response
                response.data = {
                    "verified": True,
                    "access_token": new_token,
                    "user_data": user_dict,
                }

        return build_api_response(response)

    except HTTPException as e:
        # Handle token expiry specifically with user-friendly message
        if e.status_code == http.HTTPStatus.UNAUTHORIZED:
            error_detail = e.detail
            if isinstance(error_detail, dict) and "Token has expired" in str(error_detail.get("message", "")):
                return build_api_response(
                    GenericResponseModel(
                        status_code=http.HTTPStatus.UNAUTHORIZED,
                        status=False,
                        message="Your session has expired. Please login again to continue.",
                    )
                )
        # Re-raise other HTTPExceptions
        raise
    except Exception as e:
        from logger import logger
        logger.error(
            msg=f"Unexpected error during OTP verification: {str(e)}",
            exc_info=True
        )
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )
        )


@otp_router.post(
    "/resend",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def resend_otp(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    _=Depends(build_request_context),
):
    """
    Resend OTP to user during registration/login flow.
    This endpoint does NOT require full authentication - only a valid JWT token.
    """

    try:
        # Decode token to get user_id (without full authentication check)
        token = credentials.credentials
        payload = JWTHandler.decode_token_without_context(token)
        user_id = payload.get("id")

        if not user_id:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    status=False,
                    message="Invalid token: user_id not found",
                )
            )

        # Resend OTP for this user
        response: GenericResponseModel = OTPHandler.generate_and_send_otp(
            user_id=user_id,
            otp_type=OTPHandler.OTP_TYPE_PHONE,
            purpose="user_registration_resend",
        )

        return build_api_response(response)

    except HTTPException as e:
        # Handle token expiry specifically with user-friendly message
        if e.status_code == http.HTTPStatus.UNAUTHORIZED:
            error_detail = e.detail
            if isinstance(error_detail, dict) and "Token has expired" in str(error_detail.get("message", "")):
                return build_api_response(
                    GenericResponseModel(
                        status_code=http.HTTPStatus.UNAUTHORIZED,
                        status=False,
                        message="Your session has expired. Please login again to continue.",
                    )
                )
        # Re-raise other HTTPExceptions
        raise
    except Exception as e:
        from logger import logger
        logger.error(
            msg=f"Unexpected error during OTP resend: {str(e)}",
            exc_info=True
        )
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )
        )


@otp_router.post(
    "/update-phone",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def update_phone_number(
    phone_update: PhoneNumberUpdateModel,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    _=Depends(build_request_context),
):
    """
    Update phone number for unverified user during OTP verification flow.
    This endpoint does NOT require full authentication - only a valid JWT token.
    After updating phone number, a new OTP will be sent automatically.
    """

    try:
        # Decode token to get user_id (without full authentication check)
        token = credentials.credentials
        payload = JWTHandler.decode_token_without_context(token)
        user_id = payload.get("id")

        if not user_id:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    status=False,
                    message="Invalid token: user_id not found",
                )
            )

        # Update phone number and send new OTP
        response: GenericResponseModel = OTPHandler.update_phone_and_resend_otp(
            user_id=user_id,
            new_phone=phone_update.phone_number,
            allow_verified_users=False,
        )

        return build_api_response(response)

    except HTTPException as e:
        # Handle token expiry specifically with user-friendly message
        if e.status_code == http.HTTPStatus.UNAUTHORIZED:
            error_detail = e.detail
            if isinstance(error_detail, dict) and "Token has expired" in str(error_detail.get("message", "")):
                return build_api_response(
                    GenericResponseModel(
                        status_code=http.HTTPStatus.UNAUTHORIZED,
                        status=False,
                        message="Your session has expired. Please login again to continue.",
                    )
                )
        # Re-raise other HTTPExceptions
        raise
    except Exception as e:
        from logger import logger
        logger.error(
            msg=f"Unexpected error during phone number update: {str(e)}",
            exc_info=True
        )
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )
        )


@otp_router.get(
    "/status",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_otp_status(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    _=Depends(build_request_context),
):
    """
    Get OTP status for user including time since last OTP sent.
    Used for frontend timer validation.
    """
    try:
        # Decode token to get user_id
        token = credentials.credentials
        payload = JWTHandler.decode_token_without_context(token)
        user_id = payload.get("id")

        if not user_id:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.UNAUTHORIZED,
                    status=False,
                    message="Invalid token: user_id not found",
                )
            )

        # Get OTP status
        response: GenericResponseModel = OTPHandler.get_otp_status(user_id)

        return build_api_response(response)

    except HTTPException as e:
        # Handle token expiry specifically with user-friendly message
        if e.status_code == http.HTTPStatus.UNAUTHORIZED:
            error_detail = e.detail
            if isinstance(error_detail, dict) and "Token has expired" in str(error_detail.get("message", "")):
                return build_api_response(
                    GenericResponseModel(
                        status_code=http.HTTPStatus.UNAUTHORIZED,
                        status=False,
                        message="Your session has expired. Please login again to continue.",
                    )
                )
        # Re-raise other HTTPExceptions
        raise
    except Exception as e:
        from logger import logger
        logger.error(
            msg=f"Unexpected error getting OTP status: {str(e)}",
            exc_info=True
        )
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )
        )
