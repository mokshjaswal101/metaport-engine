import http
from fastapi import APIRouter, Request, Depends, HTTPException
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import Any
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Request
from typing import Optional
from fastapi import Query
from starlette.responses import PlainTextResponse
from context_manager.context import build_request_context
from uuid import UUID

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .whatsapp_service import WhatsappService

# creating a client router
whatsapp_router = APIRouter(tags=["whatsapp"])

security = HTTPBearer()


@whatsapp_router.post(
    "/whatsapp/webhook",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def tracking_webhook(request: Request):

    track_req = await request.json()

    print(track_req)

    return GenericResponseModel(status=True, status_code=200, message="success")

    # response: GenericResponseModel = Delhivery.tracking_webhook(track_req=track_req)
    return build_api_response(response)


@whatsapp_router.post(
    "/whatsapp/order-confirmation/confirm",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def confirm_order_via_whatsapp(id: UUID):

    response: GenericResponseModel = WhatsappService.confirm_order(id=id)
    return build_api_response(response)


@whatsapp_router.post(
    "/whatsapp/order-confirmation/cancel",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def confirm_order_via_whatsapp(id: UUID):

    response: GenericResponseModel = WhatsappService.cancel_order(id=id)
    return build_api_response(response)
