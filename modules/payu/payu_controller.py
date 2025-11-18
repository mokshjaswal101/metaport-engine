import http
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from fastapi import APIRouter, Depends
from fastapi.responses import RedirectResponse
from typing import Any
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .payu_service import PayU

# creating a client router
payu_router = APIRouter(tags=["payu"], prefix="/payu")

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    token = credentials.credentials
    payload = JWTHandler.decode_access_token(token)
    return payload


# @payu_router.post(
#     "/verify-payment",
#     status_code=http.HTTPStatus.OK,
#     response_model=GenericResponseModel,
#     dependencies=[Depends(get_current_user)],
# )
# async def verify(payment_req: RazorPayValidateRequest):
#     response: GenericResponseModel = PayU.verify_payment(payment_req=payment_req)
#     return build_api_response(response)


@payu_router.post(
    "/webhook/payment/success",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def update_payment_status(request: Request):

    payment_req = await request.json()

    print("Payment Request:", payment_req)

    response: GenericResponseModel = PayU.payment_status_webhook_success(
        payment_req=payment_req
    )
    return build_api_response(response)


@payu_router.post(
    "/webhook/payment/failed",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def update_payment_status(request: Request):

    payment_req = await request.json()

    print("Payment Request:", payment_req)

    response: GenericResponseModel = PayU.payment_status_webhook_failed(
        payment_req=payment_req
    )
    return build_api_response(response)


@payu_router.post("/payments/success")
async def payu_success(request: Request):
    form = await request.form()
    # 1) verify the hash / transaction status here
    # 2) update your order in the database
    # 3) then redirect the user to your SPA’s success page
    return RedirectResponse(url="https://app.lastmiles.co/orders", status_code=303)


@payu_router.post("/payments/failure")
async def payu_failure(request: Request):
    form = await request.form()
    # handle failure, logging, etc.
    return RedirectResponse(url="https://app.lastmiles.co/orders", status_code=303)
