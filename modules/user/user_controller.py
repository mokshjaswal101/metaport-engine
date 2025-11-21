import http
from fastapi import APIRouter
from fastapi import APIRouter

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .user_schema import (
    UserInsertModel,
    ChangePasswordModel,
)

# utils
from utils.response_handler import build_api_response

# service
from .user_service import UserService


# creating a client router
user_router = APIRouter(tags=["user"], prefix="/user")


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


# @user_router.post(
#     "/profile/change-password",
#     status_code=http.HTTPStatus.CREATED,
#     response_model=GenericResponseModel,
# )
# async def change_user_password(
#     user_data: ChangePasswordModel,
# ):
#     response: GenericResponseModel = UserService.change_password(user_data=user_data)
#     return build_api_response(response)


@user_router.post(
    "/profile/change-password",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def change_user_password(
    user_data: ChangePasswordModel,
):
    # Call async service method
    response: GenericResponseModel = await UserService.change_password(
        user_data=user_data
    )
    return build_api_response(response)
