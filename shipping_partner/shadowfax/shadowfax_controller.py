import http
from fastapi import APIRouter, Request, Depends, HTTPException
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import Any
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .shadowfax import Shadowfax

# creating a client router
shadowfax_router = APIRouter(tags=["shadowfax"])

security = HTTPBearer()


@shadowfax_router.post(
    "/webhook/courier/shadowfax/track-order",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def tracking_webhook(request: Request):

    track_req = await request.json()

    print("track_req", track_req)

    response: GenericResponseModel = Shadowfax.tracking_webhook(track_req=track_req)
    return build_api_response(response)
