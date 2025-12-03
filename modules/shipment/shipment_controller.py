import http
from fastapi import APIRouter
from typing import List

from context_manager.context import build_request_context

# schema
from schema.base import GenericResponseModel
from modules.shipment.shipment_schema import (
    CreateShipmentModel,
    generateLabelRequest,
    BulkCreateShipmentModel,
    CancelshipmentRequestSchema,
    NewBulkCreateShipmentModel,
    ShippingChargesGetSchema,
)

# utils
from utils.response_handler import build_api_response

# services
from .shipment_service import ShipmentService


# Creating the router for orders
shipment_router = APIRouter(prefix="/shipment", tags=["shipments"])
track_router = APIRouter(prefix="/shipment/external", tags=["Tracking"])


@shipment_router.post(
    "/calculate-shipping-zone",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def create_order(pickup_pincode: int, destination_pincode: int):
    try:
        response: GenericResponseModel = ShipmentService.calculate_shipping_zone(
            pickup_pincode=pickup_pincode, destination_pincode=destination_pincode
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while posting the shipment.",
            )
        )


# create a new shipment
# @shipment_router.post(
#     "/assign-awb",
#     status_code=http.HTTPStatus.CREATED,
#     response_model=GenericResponseModel,
# )
# async def assign_awb(shipment_params: CreateShipmentModel):
#     try:
#         response: GenericResponseModel = ShipmentService.assign_awb(
#             shipment_params=shipment_params
#         )
#         return build_api_response(response)

#     except Exception as e:
#         return build_api_response(
#             GenericResponseModel(
#                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#                 data=str(e),
#                 message="An error occurred while assigning awb.",
#             )
#         )


@shipment_router.post(
    "/assign-awb",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def assign_awb(shipment_params: CreateShipmentModel):
    try:
        response = await ShipmentService.assign_awb(shipment_params=shipment_params)
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while assigning awb.",
            )
        )


# create a new shipment
@shipment_router.post(
    "/assign-reverse-awb",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
async def assign_awb(shipment_params: CreateShipmentModel):
    try:
        # If your service function is async, use await:
        response: GenericResponseModel = await ShipmentService.assign_reverse_awb(
            shipment_params=shipment_params
        )

        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while assigning awb.",
            )
        )


# create a new shipment
@shipment_router.post(
    "/bulk/assign-awb",
    status_code=http.HTTPStatus.CREATED,
    response_model=GenericResponseModel,
)
# async def bulk_assign_awb(shipment_params: BulkCreateShipmentModel):
async def bulk_assign_awb(shipment_params: NewBulkCreateShipmentModel):
    try:
        print("welcome to bulk action trigger")
        response: GenericResponseModel = ShipmentService.dev_bulk_assign_awbs(
            shipment_params=shipment_params
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while assigning awb.",
            )
        )


@shipment_router.get(
    "/track/awb/{awb_number}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def track_shipment(awb_number: str):
    try:
        response: GenericResponseModel = ShipmentService.track_shipment(
            awb_number=awb_number
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while tracking the shipment.",
            )
        )


@track_router.get(
    "/track/awb/{awb_number}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def external_track_shipment(awb_number: str):
    try:

        response: GenericResponseModel = ShipmentService.external_track_shipment(
            awb_number=awb_number
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while tracking the shipment.",
            )
        )


@shipment_router.get(
    "/tracking/awb/{awb_number}",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def external_track_shipment(awb_number: str):
    try:

        response: GenericResponseModel = ShipmentService.external_track_only_shipment(
            awb_number=awb_number
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while tracking the shipment.",
            )
        )


@shipment_router.post(
    "/cancel/awbs",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def cancel_shipments(cancel_shipment_request: CancelshipmentRequestSchema):
    try:
        response: GenericResponseModel = ShipmentService.cancel_shipments(
            awb_numbers=cancel_shipment_request.awb_numbers
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Could not cancel shipment",
            )
        )


@shipment_router.post(
    "/reverse/cancel/awbs",
    status_code=http.HTTPStatus.OK,
    response_model=GenericResponseModel,
)
async def reverse_cancel_shipments(
    cancel_shipment_request: CancelshipmentRequestSchema,
):
    try:
        response: GenericResponseModel = await ShipmentService.reverse_cancel_shipments(
            awb_numbers=cancel_shipment_request.awb_numbers
        )

        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="Could not cancel shipment",
            )
        )


# @shipment_router.post("/download-manifest/", status_code=http.HTTPStatus.OK)
# async def download_manifest(download_manifest_request: generateLabelRequest):
#     try:
#         response: GenericResponseModel = ShipmentService.download_manifest(
#             order_ids=download_manifest_request.order_ids
#         )
#         return response

#     except Exception as e:
#         return build_api_response(
#             GenericResponseModel(
#                 status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#                 data=str(e),
#                 message="An error occurred while downloading manifest.",
#             )
#         )


@shipment_router.post("/download-manifest/", status_code=http.HTTPStatus.OK)
async def download_manifest(download_manifest_request: generateLabelRequest):
    try:
        response: GenericResponseModel = await ShipmentService.download_manifest(
            order_ids=download_manifest_request.order_ids
        )
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while downloading manifest.",
            )
        )


@shipment_router.post("/shipping-charges", status_code=http.HTTPStatus.OK)
async def get_shipping_charges(shipping_charges: ShippingChargesGetSchema):
    try:
        print("pass controller")
        response: GenericResponseModel = ShipmentService.get_shipping_charges(
            shipping_charges=shipping_charges
        )
        return build_api_response(response)

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while getting the shipping charges.",
            )
        )
