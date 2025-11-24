import http
from fastapi import APIRouter, Depends
from schema.base import GenericResponseModel

from context_manager.context import build_request_context

# schema
from .auth_schema import UserLoginModel, UserRegisterModel

# utils
from utils.response_handler import build_api_response

# service
from .auth_service import AuthService

# creating an auth router
auth_router = APIRouter(tags=["auth"], prefix="/api/v1")


@auth_router.post(
    "/login",
    status_code=http.HTTPStatus.OK,
)
async def login_user(
    user_login_data: UserLoginModel,
    _=Depends(build_request_context),
):

    response: GenericResponseModel = AuthService.login_user(
        user_login_data=user_login_data
    )

    return build_api_response(response)


@auth_router.post(
    "/signup",
    status_code=http.HTTPStatus.OK,
)
async def signup_user(
    client_data: UserRegisterModel,
    _=Depends(build_request_context),
):

    response: GenericResponseModel = AuthService.signup(client_data=client_data)

    return build_api_response(response)


@auth_router.post(
    "/dev/login",
    status_code=http.HTTPStatus.OK,
)
async def dev_login_user(
    user_login_data: UserLoginModel,
    _=Depends(build_request_context),
):
    """Development login endpoint (same as /login but for testing)"""
    response: GenericResponseModel = AuthService.login_user(
        user_login_data=user_login_data
    )

    return build_api_response(response)
