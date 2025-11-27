import http
from fastapi import APIRouter
from typing import Any, List, Dict
from datetime import datetime


# schema
from schema.base import GenericResponseModel
from modules.returns.return_schema import (
    BulkReturnOrderRequest,
    Order_create_request_model,
    Order_filters,
    Order_Status_Filters,
    Order_Export_Filters,
    bulkCancelOrderModel,
    Get_Order_Usging_AWB_OR_Order_Id,
    Dev_Return_Order_Create_Request_Model,
)

# utils
from utils.response_handler import build_api_response

# services
from .return_service import ReturnService


# Creating the router for orders
return_router = APIRouter(prefix="/returns", tags=["returns"])
special_returns_router = APIRouter(tags=["special-returns"])


# create a new order
@return_router.post(
    "/create",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(
    order_data: Order_create_request_model,
):
    try:
        # Await the async service
        response: GenericResponseModel = await ReturnService.create_order(
            order_data=order_data
        )

        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
                status=False,
            )
        )


# Bulk create return orders
@return_router.post(
    "/bulk-create-returns",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def bulk_create_return_orders(data: BulkReturnOrderRequest):
    """
    Create return orders for multiple forward orders in bulk.
    Only creates returns for delivered orders.
    """
    try:
        response: GenericResponseModel = await ReturnService.bulk_create_return_orders(
            order_ids=data.order_ids
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating bulk return orders.",
                status=False,
            )
        )


# create a new order
@return_router.post(
    "/getOrderUsingAorOrderId",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_using_awb_or_order_Id_order(
    get_Order_Usging_AWB_OR_Order_Id: Get_Order_Usging_AWB_OR_Order_Id,
):
    try:
        response: GenericResponseModel = ReturnService.Get_Order_Using_Awb_OR_OrderId(
            get_Order_Usging_AWB_OR_Order_Id=get_Order_Usging_AWB_OR_Order_Id
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


# Edit order
@return_router.post(
    "/update/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def update_order(order_id: str, order_data: Order_create_request_model):
    try:
        response: GenericResponseModel = ReturnService.update_order(
            order_id=order_id, order_data=order_data
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


# Bulk import order
@return_router.post(
    "/bulk-import",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def bulk_import(
    orders_data: List[Order_create_request_model],
):  # No data validation is done here, will be done individually in service  to generate error file
    try:
        response: GenericResponseModel = ReturnService.bulk_import(orders=orders_data)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while importing the orders.",
            )
        )


# cancel an order
@return_router.post(
    "/cancel/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(order_id: str):
    try:
        response: GenericResponseModel = ReturnService.cancel_order(order_id=order_id)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while cancelling the order.",
            )
        )


# cancel an order
@return_router.post(
    "/bulk/cancel",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(data: bulkCancelOrderModel):
    try:
        response: GenericResponseModel = ReturnService.bulk_cancel_order(
            order_ids=data.order_ids
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while cancelling the orders.",
            )
        )


# get all orders for a user
@return_router.post(
    "/",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_all_orders(order_filters: Order_filters):
    try:
        response: GenericResponseModel = await ReturnService.get_all_orders(
            order_filters=order_filters
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching the orders.",
            )
        )


# get all orders for a user
@return_router.post(
    "/export",
    status_code=http.HTTPStatus.CREATED,
)
async def export_orders(order_filters: Order_Export_Filters):
    try:
        response: GenericResponseModel = ReturnService.export_orders(
            order_filters=order_filters
        )
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while export report.",
            )
        )


# get the status count of all ordes
@return_router.post(
    "/status",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_status_counts(order_status_filter: Order_Status_Filters):
    try:
        response: GenericResponseModel = ReturnService.get_order_status_counts(
            order_status_filter=order_status_filter
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching the orders status.",
            )
        )


# get the status count of all ordes
@return_router.post(
    "/cod-remittance",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_status_counts():
    try:
        response: GenericResponseModel = ReturnService.get_remittance()
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching the orders status.",
            )
        )


# get the status count of all ordes
@return_router.get(
    "/cod-remittance/orders",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_status_counts(id: int):
    try:
        response: GenericResponseModel = ReturnService.get_remittance_orders(
            cycle_id=id
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching the orders status.",
            )
        )


# get details of one order
@return_router.get(
    "/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_by_id(order_id: str):
    try:
        response: GenericResponseModel = ReturnService.get_order_by_Id(
            order_id=order_id
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching order details.",
            )
        )


@return_router.post(
    "/temp",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def put_order(order: Dict[Any, Any]):
    try:
        response: GenericResponseModel = ReturnService.order_put(order_data=order)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching order details.",
            )
        )


# DEV ROUTES - Similar to order dev routes
@special_returns_router.post(
    "/dev/returns/create",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def dev_create_return_order(
    order_data: Dev_Return_Order_Create_Request_Model,
):
    """Development endpoint for creating return orders with shadowfax courier"""
    try:
        response: GenericResponseModel = ReturnService.dev_create_return_order(
            order_data=order_data
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the return order.",
            )
        )


@special_returns_router.post(
    "/dev/returns/cancel/awbs",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def dev_cancel_return_awbs():
    """Development endpoint for canceling return AWBs"""
    try:
        response: GenericResponseModel = ReturnService.dev_cancel_return_awbs()
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while canceling the return AWBs.",
            )
        )
