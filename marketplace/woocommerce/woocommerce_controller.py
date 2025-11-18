import http
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer

from context_manager.context import build_request_context

from logger import logger


# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .woocommerce import Woocommerce

# creating a client router
woocommerce_router = APIRouter(tags=["woocommerce"])

security = HTTPBearer()


@woocommerce_router.post(
    "/order/webhook",
)
async def webhook(request: Request):
    try:
        order_data = await request.json()
        company_id = request.query_params.get("company_id")

        response = Woocommerce.create_order(order_data, com_id=company_id)
        return response
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@woocommerce_router.post("/woocommerce/add_webhook")
async def add_marketplace(_=Depends(build_request_context)):
    try:
        response = await Woocommerce.add_marketplace()
        return response
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Credentials are not capable to make connection",
                # details="Credentials are not capable of making connections.",
            )
        )


@woocommerce_router.post(
    "/remove_webhook",
    status_code=http.HTTPStatus.CREATED,
    # response_model=GenericResponseModel,
)
async def remove_webhook(request: Request, _=Depends(build_request_context)):
    try:

        response = await Woocommerce.remove_webhook(request)
        return response
    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )
