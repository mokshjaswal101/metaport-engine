import http
from fastapi import APIRouter, Request, Depends, HTTPException
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import Any
import json
import ast
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import Order_Model
from shipping_partner.xpressbees.xpressbees_schema import (
    Xpressbees_order_create_model,
    Xpressbees_order_cancel_model,
    Xpressbees_track_order_model,
)

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler


# service
from .xpressbees import Xpressbees

from logger import logger

# creating a client router
xpressbees_router = APIRouter(tags=["Xpressbees"])

security = HTTPBearer()


# orderTrack


@xpressbees_router.post(
    "/webhook/courier/xbees/track-order",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def tracking_webhook(request: Request):

    track_req = await request.json()

    print("courier_webhook_hit", track_req)

    response: GenericResponseModel = Xpressbees.tracking_webhook(track_req=track_req)
    return build_api_response(response)


@xpressbees_router.post(
    "/contract/transfer",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def contract_transfer():
    response: GenericResponseModel = Xpressbees.contract_transfer()
    return build_api_response(response)
