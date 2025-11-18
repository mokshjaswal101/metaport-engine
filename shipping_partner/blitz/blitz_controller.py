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


from logger import logger

# creating a client router
blitz_router = APIRouter(tags=["ATS"])

security = HTTPBearer()

from .blitz import Blitz


# orderTrack


@blitz_router.post(
    "/webhook/courier/blitz/track-order",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def tracking_webhook(request: Request):

    track_req = await request.json()

    print("track_req", track_req)

    # return build_api_response(
    #     GenericResponseModel(status=True, status_code=200, message="success")
    # )

    response: GenericResponseModel = Blitz.tracking_webhook(
        track_req=track_req,
    )
    return build_api_response(response)
