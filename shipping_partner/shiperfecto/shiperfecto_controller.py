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
from .shiperfecto import Shiperfecto

# creating a client router
shiperfecto_router = APIRouter(tags=["shiperfecto"])

security = HTTPBearer()


@shiperfecto_router.post(
    "/webhook/courier/shiperfecto/track-order",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def tracking_webhook(request: Request):

    track_req = await request.json()

    response: GenericResponseModel = Shiperfecto.tracking_webhook(track_req=track_req)
    return build_api_response(response)
