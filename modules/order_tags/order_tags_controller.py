import http
from typing import Any, List, Dict
from datetime import datetime
from fastapi import (
    APIRouter,
    File,
    Form,
)


# schema
from schema.base import GenericResponseModel
from modules.order_tags.order_tags_schema import OrderTagsBaseModel

# utils
from utils.response_handler import build_api_response

# services
from .order_tags_service import OrderTagsService


# Creating the router for orders
order_tags_router = APIRouter(prefix="/order-tags", tags=["order tags"])


# create a new order
@order_tags_router.post(
    "/create",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(
    tags_data: OrderTagsBaseModel,
):
    try:
        response: GenericResponseModel = OrderTagsService.create_tag(
            tags_data=tags_data
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order tag.",
            )
        )
