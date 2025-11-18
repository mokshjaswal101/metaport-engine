import http
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from typing import Any
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from .razorpay_schema import RazorPayValidateRequest

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .razorpay_service import Razorpay
from ..razorpay.razorpay_service import Razorpay

# creating a client router
razorpay_router = APIRouter(tags=["razorpay"], prefix="/razorpay")

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    token = credentials.credentials
    payload = JWTHandler.decode_access_token(token)
    return payload


@razorpay_router.post(
    "/verify-payment",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
    dependencies=[Depends(get_current_user)],
)
async def verify(payment_req: RazorPayValidateRequest):
    response: GenericResponseModel = Razorpay.verify_payment(payment_req=payment_req)
    return build_api_response(response)


@razorpay_router.post(
    "/webhook/payment",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def update_payment_status(request: Request):

    razorpay_payment_req = await request.json()

    response: GenericResponseModel = Razorpay.payment_status_webhook(
        payment_req=razorpay_payment_req
    )
    return build_api_response(response)
