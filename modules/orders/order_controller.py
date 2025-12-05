import http
from typing import Any, List, Dict
from fastapi import (
    APIRouter,
)


# schema
from schema.base import GenericResponseModel
from modules.orders.order_schema import (
    Order_create_request_model,
    Order_filters,
    Order_Status_Filters,
    Order_Export_Filters,
    bulkCancelOrderModel,
    BulkDimensionUpdateModel,
    UpdatePickupLocationModel,
)

# utils
from utils.response_handler import build_api_response

# services
from .order_service import OrderService


# Creating the router for orders
order_router = APIRouter(prefix="/orders", tags=["orders"])
special_orders_router = APIRouter(tags=["special-orders"])


# create a new order
@order_router.post(
    "/create",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(
    order_data: Order_create_request_model,
):
    try:
        response: GenericResponseModel = OrderService.create_order(
            order_data=order_data
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
@order_router.post(
    "/update/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def update_order(order_id: str, order_data: Order_create_request_model):
    try:
        response: GenericResponseModel = OrderService.update_order(
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


# get the list of customers with same phone number
@order_router.get(
    "/get-customers",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_customers(phone: str):
    try:

        response: GenericResponseModel = OrderService.get_customers(phone=phone)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching the customer.",
            )
        )


@order_router.get(
    "/previous-orders/{order_id}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_previous_orders(
    order_id: str, page_number: int = 1, batch_size: int = 10
):
    """
    Get previous orders for the same phone number as the given order ID with pagination
    """

    try:
        response: GenericResponseModel = OrderService.get_previous_orders(
            order_id=order_id, page_number=page_number, batch_size=batch_size
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching previous orders.",
            )
        )


# Edit order
@order_router.delete(
    "/delete/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def update_order(order_id: str):
    try:
        response: GenericResponseModel = OrderService.delete_order(order_id=order_id)
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
@order_router.post(
    "/bulk-import",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def bulk_import(
    orders_data: List[dict],  # Accept raw dict data instead of validated models
):
    try:
        print("popopo")
        response: GenericResponseModel = OrderService.bulk_import(orders=orders_data)
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
@order_router.post(
    "/cancel/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(order_id: str):
    try:
        response: GenericResponseModel = OrderService.cancel_order(order_id=order_id)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while cancelling the order.",
            )
        )


# clone an order
@order_router.post(
    "/clone/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def clone_order(order_id: str):
    try:
        response: GenericResponseModel = OrderService.clone_order(order_id=order_id)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while duplicating the order.",
            )
        )


# cancel an order
@order_router.post(
    "/bulk/cancel",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(data: bulkCancelOrderModel):
    try:
        response: GenericResponseModel = OrderService.bulk_cancel_order(
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
@order_router.post(
    "/",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_all_orders(order_filters: Order_filters):
    try:

        response: GenericResponseModel = OrderService.get_all_orders(
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
@order_router.post(
    "/export",
    status_code=http.HTTPStatus.CREATED,
)
async def export_orders(order_filters: Order_Export_Filters):
    try:
        response: GenericResponseModel = OrderService.export_orders(
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
# @order_router.post(
#     "/status",
#     status_code=http.HTTPStatus.CREATED,
#     response_model=GenericResponseModel,
# )
# async def get_order_status_counts(order_status_filter: Order_Status_Filters):
#     try:
#         response: GenericResponseModel = OrderService.get_order_status_counts(
#             order_status_filter=order_status_filter
#         )
#         return build_api_response(response)

#     except Exception as e:
#         return build_api_response(
#             GenericResponseModel(
#                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#                 data=str(e),
#                 message="An error occurred while fetching the orders status.",
#             )
#         )


# get the status count of all ordes
@order_router.post(
    "/cod-remittance",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_status_counts():
    try:
        response: GenericResponseModel = OrderService.get_remittance()
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
@order_router.get(
    "/cod-remittance/orders",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_status_counts(id: int):
    try:
        response: GenericResponseModel = OrderService.get_remittance_orders(cycle_id=id)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching the orders status.",
            )
        )


@order_router.post(
    "/temp",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def put_order(order: Dict[Any, Any]):
    try:
        response: GenericResponseModel = OrderService.order_put(order_data=order)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching order details.",
            )
        )


@order_router.post(
    "/bulk-update-dimensions",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def update_Dimentions(bulkDimensionsUpdate: BulkDimensionUpdateModel):
    try:
        response: GenericResponseModel = OrderService.bulk_update_Dimensions(
            bulkDimensionsUpdate=bulkDimensionsUpdate
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Could not update orders",
            )
        )


@order_router.post(
    "/update-pickup-address",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def update_location_code(update_pickup_payload: UpdatePickupLocationModel):
    response: GenericResponseModel = OrderService.update_pickup_location(
        update_pickup_payload=update_pickup_payload
    )
    return build_api_response(response)


@order_router.get(
    "/bulk-upload-logs",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def get_bulk_upload_logs():
    try:
        print("=== BULK UPLOAD LOGS CONTROLLER REACHED ===")

        response: GenericResponseModel = OrderService.get_bulk_upload_logs()
        print(f"Response status: {response.status_code}")
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching bulk upload logs.",
            )
        )


# keep this at the bottom to avoid shadowing static routes
@order_router.get(
    "/{order_id}",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def get_order_by_id(order_id: str):
    try:
        response: GenericResponseModel = OrderService.get_order_by_Id(order_id=order_id)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while fetching order details.",
            )
        )


# Terminate idle database connections
@special_orders_router.post(
    "/dev/terminate-idle-db-connections",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def terminate_idle_database_connections():
    """
    Administrative endpoint to terminate idle database connections.
    This helps clean up idle connections that may be consuming database resources.
    """
    try:
        result = OrderService.terminate_idle_database_connections()

        if result["status"] == "success":
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    data={"terminated_connections": result["terminated_connections"]},
                    message=result["message"],
                    status=True,
                )
            )
        else:
            return build_api_response(
                GenericResponseModel(
                    status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                    data=None,
                    message=result["message"],
                    status=False,
                )
            )

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while terminating idle database connections.",
                status=False,
            )
        )
