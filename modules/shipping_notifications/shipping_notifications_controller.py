import http
from fastapi import APIRouter


# schema
from schema.base import GenericResponseModel
from .shipping_notifications_schema import ShippingNotificationSettingBaseModel

# utils
from utils.response_handler import build_api_response

# services
from .shipping_notifications_service import ShippingNotificaitions


notifications_router = APIRouter(
    tags=["shipping_notifications"], prefix="/notifications"
)


# @notifications_router.get(
#     "/balance",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def get_notifications_balance():
#     response: GenericResponseModel = ShippingNotificaitions.get_notifications_balance()
#     return build_api_response(response)


@notifications_router.get(
    "/balance",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_notifications_balance():
    response: GenericResponseModel = (
        await ShippingNotificaitions.get_notifications_balance()
    )
    return build_api_response(response)


# @notifications_router.get(
#     "/rates",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def get_notification_rates():
#     response: GenericResponseModel = ShippingNotificaitions.get_notification_rates()
#     return build_api_response(response)


@notifications_router.get(
    "/rates",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_notification_rates():
    response: GenericResponseModel = (
        await ShippingNotificaitions.get_notification_rates()
    )
    return build_api_response(response)


# @notifications_router.get(
#     "/setting",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def get_notification_setting():
#     response: GenericResponseModel = ShippingNotificaitions.get_notifications_settings()
#     return build_api_response(response)


# @notifications_router.post(
#     "/setting",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
# )
# async def update_shipping_notification_setting(
#     settings: ShippingNotificationSettingBaseModel,
# ):
#     response: GenericResponseModel = (
#         ShippingNotificaitions.update_notification_settings(settings=settings)
#     )
#     return build_api_response(response)


@notifications_router.get(
    "/setting",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_notification_setting():
    response: GenericResponseModel = (
        await ShippingNotificaitions.get_notifications_settings()
    )
    return build_api_response(response)


@notifications_router.post(
    "/setting",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def update_shipping_notification_setting(
    settings: ShippingNotificationSettingBaseModel,
):
    response: GenericResponseModel = (
        await ShippingNotificaitions.update_notification_settings(settings=settings)
    )
    return build_api_response(response)
