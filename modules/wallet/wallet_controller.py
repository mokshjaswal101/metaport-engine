import http
from fastapi import APIRouter
from fastapi import APIRouter
from typing import Any

# schema
from schema.base import GenericResponseModel
from .wallet_schema import walletOptionsSchema, log_filters, rechargeRecordFilters
from ..payu.payu_schema import PaymentRequest

# utils
from utils.response_handler import build_api_response

# service
from .wallet_service import WalletService
from ..razorpay.razorpay_service import Razorpay
from ..payu.payu_service import PayU

# creating a client router
wallet_router = APIRouter(tags=["wallet"], prefix="/wallet")


@wallet_router.post(
    "/recharge",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def recharge_wallet(wallet_options: walletOptionsSchema):
    response: GenericResponseModel = Razorpay.create_order(
        amount=wallet_options.amount, wallet_type=wallet_options.wallet_type
    )
    return build_api_response(response)


@wallet_router.post(
    "/payu/recharge",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def recharge_wallet(params: PaymentRequest):
    print("1")
    response: GenericResponseModel = PayU.create_payment(payment_request=params)
    return build_api_response(response)


@wallet_router.get(
    "/",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_balance():
    response: GenericResponseModel = WalletService.get_balance()
    return build_api_response(response)


@wallet_router.post(
    "/logs",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_wallet_logs(filters: log_filters):
    response: GenericResponseModel = WalletService.get_wallet_logs(filters=filters)
    return build_api_response(response)


@wallet_router.post(
    "/recharge-records",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_wallet_logs(filters: rechargeRecordFilters):
    response: GenericResponseModel = WalletService.get_recharge_records(filters=filters)
    return build_api_response(response)
